package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;

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

public class CSVImportMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private static final Log LOG = LogFactory.getLog(CSVImportMapper.class);

    private static final String ERROR_FILE = "error.csv";

    private Schema schema;

    private Table table;

    private Path outputPath;

    private Configuration conf;

    private boolean missingRequiredColValue = Boolean.FALSE;
    private boolean fieldMalFormed = Boolean.FALSE;

    private Map<String, String> errorMap = new HashMap<>();

    private CSVPrinter csvFilePrinter;

    private int errorLineNumber;

    private long importedLineNum;

    private String avroFileName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        schema = AvroJob.getOutputKeySchema(conf);
        LOG.info("schema is: " + schema.toString());

        table = JsonUtils.deserialize(conf.get("eai.table.schema"), Table.class);
        LOG.info("table is:" + table);

        outputPath = MapFileOutputFormat.getOutputPath(context);
        LOG.info("Path is:" + outputPath);

        csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE), CSVFormat.RFC4180.withHeader("LineNumber",
                "ErrorMessage").withDelimiter(','));

        errorLineNumber = conf.getInt("errorLineNumber", 1000);

        if (StringUtils.isEmpty(table.getName())) {
            avroFileName = "file.avro";
        } else {
            avroFileName = table.getName() + ".avro";
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        process(context);
        cleanup(context);
    }

    private void process(Context context) throws IOException {
        @SuppressWarnings("deprecation")
        String csvFileName = getCSVFilePath(context.getLocalCacheFiles());
        if (csvFileName == null) {
            throw new RuntimeException("Not able to find csv file from localized files");
        }

        try (InputStreamReader reader = new InputStreamReader(new FileInputStream(csvFileName))) {
            CSVFormat format = CSVFormat.RFC4180.withHeader();
            try (CSVParser parser = new CSVParser(reader, format)) {
                DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
                try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
                    dataFileWriter.create(schema, new File(avroFileName));
                    for (Iterator<CSVRecord> iterator = parser.iterator();;) {
                        LOG.info("Start to processing line: " + parser.getRecordNumber());
                        importedLineNum = context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue();
                        CSVRecord csvRecord = null;
                        missingRequiredColValue = false;
                        fieldMalFormed = false;
                        try {
                            if (iterator.hasNext()) {
                                csvRecord = iterator.next();
                            } else {
                                break;
                            }
                        } catch (Exception e) {
                            LOG.error(e);
                            context.getCounter(RecordImportCounter.ROW_ERROR).increment(1);
                            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
                            continue;
                        }
                        GenericRecord avroRecord = toGenericRecord(csvRecord.toMap());
                        if (errorMap.size() == 0) {
                            dataFileWriter.append(avroRecord);
                            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
                        } else {
                            if (missingRequiredColValue) {
                                context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).increment(1);
                            } else if (fieldMalFormed) {
                                context.getCounter(RecordImportCounter.FIELD_MALFORMED).increment(1);
                            }
                            context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);
                            if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() <= errorLineNumber) {
                                csvFilePrinter.printRecord(parser.getRecordNumber() + 1, errorMap.values().toString());
                                csvFilePrinter.flush();
                            }
                            errorMap.clear();
                        }
                    }
                }
            }
        }
    }

    public String getCSVFilePath(Path[] paths) {
        for (Path path : paths) {
            if (path.getName().endsWith(".csv")) {
                return path.getName();
            }
        }
        return null;
    }

    private GenericRecord toGenericRecord(Map<String, String> csvRecord) {
        GenericRecord avroRecord = new GenericData.Record(schema);

        for (Map.Entry<String, String> entry : csvRecord.entrySet()) {
            final String csvFieldKey = entry.getKey();
            Attribute attr = table.getAttributeFromDisplayName(csvFieldKey);

            Type avroType = schema.getField(attr.getName()).schema().getTypes().get(0).getType();
            String csvFieldValue = String.valueOf(entry.getValue());
            Object avroFieldValue = null;

            List<InputValidator> validators = attr.getValidators();

            try {
                validateAttribute(validators, csvRecord, attr);
                LOG.info(String.format("Validation Passed for %s! Starting to convert to avro value.", csvFieldKey));
                if (attr.isNullable() && StringUtils.isEmpty(csvFieldValue)) {
                    avroFieldValue = null;
                } else {
                    avroFieldValue = toAvro(csvFieldValue, avroType, attr);
                }
            } catch (Exception e) {
                LOG.error(e);
                errorMap.put(csvFieldKey, e.getMessage());
            }
            avroRecord.put(attr.getName(), avroFieldValue);
        }
        avroRecord.put(InterfaceName.InternalId.name(), importedLineNum + 1);
        return avroRecord;
    }

    private void validateAttribute(List<InputValidator> validators, Map<String, String> fieldMap, Attribute attr) {
        String attrKey = attr.getName();
        if (CollectionUtils.isNotEmpty(validators)) {
            RequiredIfOtherFieldIsEmpty validator = (RequiredIfOtherFieldIsEmpty) validators.get(0);
            if (!validator.validate(attrKey, fieldMap, table)) {
                missingRequiredColValue = true;
                if (attrKey.equals(validator.otherField)) {
                    throw new RuntimeException(String.format("Required Column %s is missing value.",
                            attr.getDisplayName()));
                } else {
                    throw new RuntimeException(String.format("%s column is empty so %s cannot be empty.", table
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
                    LOG.info("Date value from csv: " + fieldCsvValue);
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
                }
            default:
                LOG.info("size is:" + fieldCsvValue.length());
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
        csvFilePrinter.close();
        HdfsUtils.copyLocalToHdfs(context.getConfiguration(), avroFileName, outputPath + "/" + avroFileName);
        if (context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue() == 0) {
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).setValue(0);
        }
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
        if (context.getCounter(RecordImportCounter.ROW_ERROR).getValue() == 0) {
            context.getCounter(RecordImportCounter.ROW_ERROR).setValue(0);
        }
    }
}
