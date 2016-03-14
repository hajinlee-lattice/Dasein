package com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.TreeMap;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.sqoop.mapreduce.AvroJob;
import org.mortbay.log.Log;

import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.SqoopRecord;
import com.cloudera.sqoop.mapreduce.AutoProgressMapper;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.metadata.SemanticType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.sqoop.csvimport.mapreduce.db.CSVDBRecordReader;

import java.util.Comparator;

/**
 * Imports records by transforming them to Avro records in an Avro data file.
 */
@SuppressWarnings("deprecation")
public class LedpCSVToAvroImportMapper extends
        AutoProgressMapper<LongWritable, SqoopRecord, AvroWrapper<GenericRecord>, NullWritable> {

    private static final String ERROR_FILE = "error.csv";

    private final AvroWrapper<GenericRecord> wrapper = new AvroWrapper<GenericRecord>();
    private Schema schema;
    private Table table;
    private List<Attribute> attributes;
    private LargeObjectLoader lobLoader;
    private Path outputPath;
    private CSVPrinter csvFilePrinter;
    private Map<String, String> errorMap;
    private String interpretation;
    private boolean emailOrWebsiteIsEmpty;
    private boolean missingRequiredColValue;
    private boolean fieldMalFormed;
    private long lineNum = 2;
    private int errorLineNumber;
    private Parser parser = new Parser();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        schema = AvroJob.getMapOutputSchema(conf);
        Log.info("schema is: " + schema.toString());
        table = JsonUtils.deserialize(conf.get("lattice.eai.file.schema"), Table.class);
        attributes = table.getAttributes();
        interpretation = table.getInterpretation();
        LOG.info("table is:" + table);
        LOG.info(interpretation);
        errorLineNumber = conf.getInt("errorLineNumber", 1000);
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        lobLoader = new LargeObjectLoader(conf, FileOutputFormat.getWorkOutputPath(context));

        outputPath = AvroOutputFormat.getOutputPath(new JobConf(context.getConfiguration()));
        Log.info("Path is:" + outputPath);
        csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE), CSVFormat.RFC4180.withHeader("LineNumber",
                "ErrorMessage").withDelimiter(','));
        CSVDBRecordReader.csvFilePrinter = csvFilePrinter;
        CSVDBRecordReader.ignoreRecordsCounter = context.getCounter(RecordImportCounter.IGNORED_RECORDS);
        CSVDBRecordReader.rowErrorCounter = context.getCounter(RecordImportCounter.ROW_ERROR);
        CSVDBRecordReader.MAX_ERROR_LINE = errorLineNumber;
        errorMap = new HashMap<>();
    }

    @Override
    protected void map(LongWritable key, SqoopRecord val, Context context) throws IOException, InterruptedException {
        try {
            Log.info("Using LedpCSVToAvroImportMapper");
            // Loading of LOBs was delayed until we have a Context.
            val.loadLargeObjects(lobLoader);
        } catch (SQLException sqlE) {
            throw new IOException(sqlE);
        }

        emailOrWebsiteIsEmpty = false;
        missingRequiredColValue = false;
        fieldMalFormed = false;
        GenericRecord record = toGenericRecord(val);
        if (errorMap.size() == 0) {
            wrapper.datum(record);
            context.write(wrapper, NullWritable.get());
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
            lineNum = context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue()
                    + context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue();
        } else {
            if (missingRequiredColValue) {
                context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).increment(1);
            } else if (fieldMalFormed) {
                context.getCounter(RecordImportCounter.FIELD_MALFORMED).increment(1);
            }
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
            lineNum = context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue()
                    + context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue();
            if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() <= errorLineNumber) {
                csvFilePrinter.printRecord(lineNum + 1, errorMap.values().toString());
                csvFilePrinter.flush();
            }
            errorMap.clear();
        }
    }

    private GenericRecord toGenericRecord(SqoopRecord val) {
        GenericRecord record = new GenericData.Record(schema);
        Map<String, Object> fieldMap = val.getFieldMap();

        Map<String, Object> sortedFieldMap = new TreeMap<>(new Comparator<String>() {

            @Override
            public int compare(String o1, String o2) {
                final String s = o1;
                Attribute attr = Iterables.find(attributes, new Predicate<Attribute>() {
                    @Override
                    public boolean apply(Attribute attribute) {
                        return attribute.getPhysicalName().equals(s);
                    }
                });
                SemanticType semanticType = attr.getSemanticType();
                if (semanticType != null
                        && (semanticType.equals(SemanticType.Email) || semanticType.equals(SemanticType.Website))) {
                    return -1;
                } else
                    return o1.compareTo(o2);
            }

        });
        sortedFieldMap.putAll(fieldMap);
        LOG.info("Start to processing line: " + (lineNum + 1));
        for (Map.Entry<String, Object> entry : sortedFieldMap.entrySet()) {
            final String fieldKey = entry.getKey();
            Attribute attr = Iterables.find(attributes, new Predicate<Attribute>() {
                @Override
                public boolean apply(Attribute attribute) {
                    return attribute.getPhysicalName().equals(fieldKey);
                }
            });

            String attrKey = attr.getName();
            Type avroType = schema.getField(attrKey).schema().getTypes().get(0).getType();
            String fieldCsvValue = String.valueOf(entry.getValue());
            Object fieldAvroValue = null;

            try {
                validateRowValueBeforeConvertToAvro(interpretation, attr, fieldCsvValue);
                LOG.info(String.format("Validation Passed for %s! Starting to convert to avro value.", attrKey));
                if (attr.isNullable() && fieldCsvValue.equals("")) {
                    fieldAvroValue = null;
                } else {
                    fieldAvroValue = toAvro(fieldCsvValue, avroType, attr);
                }
            } catch (Exception e) {
                LOG.error(e);
                errorMap.put(fieldKey, e.getMessage());
            }
            record.put(attrKey, fieldAvroValue);
        }
        return record;
    }

    private void validateRowValueBeforeConvertToAvro(String interpretation, Attribute attr, String fieldCsvValue) {
        SemanticType semanticType = attr.getSemanticType();
        if (semanticType == null) {
            Log.info("SemanticType for attribute " + attr.getName() + " is null.");
        } else if ((semanticType.equals(SemanticType.Id) || semanticType.equals(SemanticType.Event))
                && StringUtils.isEmpty(fieldCsvValue)) {
            missingRequiredColValue = true;
            throw new RuntimeException(String.format("Required Column %s is missing value.", attr.getPhysicalName()));
        } else if (interpretation.equals(SchemaInterpretation.SalesforceAccount.name())
                && semanticType.equals(SemanticType.Website) && StringUtils.isEmpty(fieldCsvValue)) {
            emailOrWebsiteIsEmpty = true;
        } else if (interpretation.equals(SchemaInterpretation.SalesforceLead.name())
                && semanticType.equals(SemanticType.Email) && StringUtils.isEmpty(fieldCsvValue)) {
            emailOrWebsiteIsEmpty = true;
        } else if (emailOrWebsiteIsEmpty
                && (semanticType.equals(SemanticType.CompanyName) || semanticType.equals(SemanticType.City)
                        || semanticType.equals(SemanticType.State) || semanticType.equals(SemanticType.Country))
                && StringUtils.isEmpty(fieldCsvValue)) {
            missingRequiredColValue = true;
            String colName = interpretation.equals(SchemaInterpretation.SalesforceAccount.name()) ? table.getAttribute(
                    SemanticType.Website).getPhysicalName() : table.getAttribute(SemanticType.Email).getPhysicalName();
            throw new RuntimeException(String.format("%s column is empty, so %s cannot be empty.", colName,
                    attr.getPhysicalName()));
        }
    }

    private Object toAvro(String fieldCsvValue, Type avroType, Attribute attr) {
        try {
            switch (avroType) {
            case DOUBLE:
                return Double.valueOf(fieldCsvValue);
            case FLOAT:
                return Float.valueOf(fieldCsvValue);
            case INT:
                return Integer.valueOf(fieldCsvValue);
            case LONG:
                if (attr.getSemanticType().equals(SemanticType.CreatedDate)
                        || attr.getSemanticType().equals(SemanticType.LastModifiedDate)
                        || (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals("Date"))) {
                    Log.info("Date value from csv: " + fieldCsvValue);
                    List<DateGroup> groups = parser.parse(fieldCsvValue);
                    List<Date> dates = groups.get(0).getDates();
                    Log.info("parse to date:" + dates.get(0));
                    return dates.get(0).getTime();
                } else {
                    return Long.valueOf(fieldCsvValue);
                }
            case STRING:
                return fieldCsvValue;
            case BOOLEAN:
                if (fieldCsvValue.equals("1")) {
                    return Boolean.TRUE;
                } else if (fieldCsvValue.equals("0")) {
                    return Boolean.FALSE;
                }
                return Boolean.valueOf(fieldCsvValue);
            case ENUM:
                return fieldCsvValue;
            default:
                fieldMalFormed = true;
                throw new RuntimeException("Not supported Field, avroType:" + avroType + ", logicalType:"
                        + attr.getLogicalDataType());
            }
        } catch (NumberFormatException e) {
            fieldMalFormed = true;
            throw new RuntimeException("Cannot convert " + fieldCsvValue + " to " + avroType + ".");
        } catch (Exception e) {
            fieldMalFormed = true;
            throw new RuntimeException("Cannot parse " + fieldCsvValue + " as Date or Timestamp.");
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException {
        if (null != lobLoader) {
            lobLoader.close();
        }
        csvFilePrinter.close();
        if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() == 0) {
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).setValue(0);
        } else {
            HdfsUtils.copyLocalToHdfs(context.getConfiguration(), ERROR_FILE, outputPath + "/" + ERROR_FILE);
        }
        if (context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).getValue() == 0) {
            context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).setValue(0);
        }
        if (context.getCounter(RecordImportCounter.FIELD_MALFORMED).getValue() == 0) {
            context.getCounter(RecordImportCounter.FIELD_MALFORMED).setValue(0);
        }
    }
}
