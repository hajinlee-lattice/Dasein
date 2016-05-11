package com.latticeengines.dataplatform.exposed.sqoop.runtime.mapreduce;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroOutputFormat;
import org.apache.avro.mapred.AvroWrapper;
import org.apache.commons.collections.CollectionUtils;
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
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;
import com.latticeengines.sqoop.csvimport.mapreduce.db.CSVDBRecordReader;

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
    private LargeObjectLoader lobLoader;
    private Path outputPath;
    private CSVPrinter csvFilePrinter;
    private Map<String, String> errorMap;
    private String interpretation;
    private boolean missingRequiredColValue;
    private boolean fieldMalFormed;
    private long lineNum;
    private int errorLineNumber;
    private long importedLineNum;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        schema = AvroJob.getMapOutputSchema(conf);
        Log.info("schema is: " + schema.toString());
        table = JsonUtils.deserialize(conf.get("eai.table.schema"), Table.class);
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

        missingRequiredColValue = false;
        fieldMalFormed = false;
        importedLineNum = context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue();
        lineNum = importedLineNum + context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() + 2;
        GenericRecord record = toGenericRecord(val);
        if (errorMap.size() == 0) {
            wrapper.datum(record);
            context.write(wrapper, NullWritable.get());
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
        } else {
            if (missingRequiredColValue) {
                context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).increment(1);
            } else if (fieldMalFormed) {
                context.getCounter(RecordImportCounter.FIELD_MALFORMED).increment(1);
            }
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
            if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() <= errorLineNumber) {
                csvFilePrinter.printRecord(lineNum, errorMap.values().toString());
                csvFilePrinter.flush();
            }
            errorMap.clear();
        }
    }

    private GenericRecord toGenericRecord(SqoopRecord val) {
        GenericRecord record = new GenericData.Record(schema);
        Map<String, Object> fieldMap = val.getFieldMap();

        LOG.info("Start to processing line: " + lineNum);
        for (Map.Entry<String, Object> entry : fieldMap.entrySet()) {
            final String fieldKey = entry.getKey();
            Attribute attr = table.getAttribute(fieldKey);

            Type avroType = schema.getField(fieldKey).schema().getTypes().get(0).getType();
            String fieldCsvValue = String.valueOf(entry.getValue());
            Object fieldAvroValue = null;

            List<InputValidator> validators = attr.getValidators();

            try {
                validateAttribute(validators, fieldMap, attr);
                LOG.info(String.format("Validation Passed for %s! Starting to convert to avro value.", fieldKey));
                if (attr.isNullable() && StringUtils.isEmpty(fieldCsvValue)) {
                    fieldAvroValue = null;
                } else {
                    fieldAvroValue = toAvro(fieldCsvValue, avroType, attr);
                }
            } catch (Exception e) {
                LOG.error(e);
                errorMap.put(fieldKey, e.getMessage());
            }
            record.put(fieldKey, fieldAvroValue);
        }
        record.put(InterfaceName.InternalId.name(), importedLineNum + 1);
        return record;
    }

    private void validateAttribute(List<InputValidator> validators, Map<String, Object> fieldMap, Attribute attr) {
        String attrKey = attr.getName();
        if (CollectionUtils.isNotEmpty(validators)) {
            RequiredIfOtherFieldIsEmpty validator = (RequiredIfOtherFieldIsEmpty) validators.get(0);
            if (!validator.validate(attrKey, fieldMap, table)) {
                missingRequiredColValue = true;
                if (attrKey.equals(validator.otherField)) {
                    throw new RuntimeException(String.format("Required Column %s is missing value.",
                            attr.getDisplayName()));
                } else {
                    throw new RuntimeException(String.format("%s column is empty, so %s cannot be empty.", table
                            .getAttribute(validator.otherField).getDisplayName(), attr.getDisplayName()));
                }
            }
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
                if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
                    Log.info("Date value from csv: " + fieldCsvValue);
                    return TimeStampConvertUtils.convertToLong(fieldCsvValue);
                } else {
                    return Long.valueOf(fieldCsvValue);
                }
            case STRING:
                return fieldCsvValue;
            case ENUM:
                return fieldCsvValue;
            case BOOLEAN:
                if (fieldCsvValue.equals("1") || fieldCsvValue.equalsIgnoreCase("true")) {
                    return Boolean.TRUE;
                } else if (fieldCsvValue.equals("0") || fieldCsvValue.equalsIgnoreCase("false")) {
                    return Boolean.FALSE;
                } else if (StringUtils.isEmpty(fieldCsvValue)) {
                    return null;
                }
            default:
                throw new IllegalArgumentException("Not supported Field, avroType:" + avroType + ", physicalDatalType:"
                        + attr.getPhysicalDataType());
            }
        } catch (IllegalArgumentException e) {
            fieldMalFormed = true;
            LOG.error(e);
            throw new RuntimeException("Cannot convert " + fieldCsvValue + " to " + avroType + ".");
        } catch (Exception e) {
            fieldMalFormed = true;
            LOG.error(e);
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
