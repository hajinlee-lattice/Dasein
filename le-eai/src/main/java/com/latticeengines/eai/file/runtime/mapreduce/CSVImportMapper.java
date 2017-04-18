package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

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
import org.springframework.format.number.NumberFormatter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.validators.InputValidator;

public class CSVImportMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private static final Log LOG = LogFactory.getLog(CSVImportMapper.class);

    private static final String ERROR_FILE = "error.csv";

    private Schema schema;

    private Table table;

    private Path outputPath;

    private Configuration conf;

    private boolean missingRequiredColValue = Boolean.FALSE;
    private boolean fieldMalFormed = Boolean.FALSE;
    private boolean rowError = Boolean.FALSE;

    private Map<String, String> errorMap = new HashMap<>();

    private CSVPrinter csvFilePrinter;

    private long lineNum = 2;

    private String avroFileName;

    private String id;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        schema = AvroJob.getOutputKeySchema(conf);
        LOG.info("schema is: " + schema.toString());

        table = JsonUtils.deserialize(conf.get("eai.table.schema"), Table.class);
        LOG.info("table is:" + table);

        outputPath = MapFileOutputFormat.getOutputPath(context);
        LOG.info("Path is:" + outputPath);

        csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE),
                LECSVFormat.format.withHeader("LineNumber", "Id", "ErrorMessage"));

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
        String csvFileName = getCSVFilePath(context.getCacheFiles());
        if (csvFileName == null) {
            throw new RuntimeException("Not able to find csv file from localized files");
        }

        String[] headers;
        try (CSVParser parser = new CSVParser(new InputStreamReader((new FileInputStream(csvFileName))),
                LECSVFormat.format)) {
            headers = new ArrayList<String>(parser.getHeaderMap().keySet()).toArray(new String[] {});
        }
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(schema, new File(avroFileName));
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName)))) {
                CSVFormat format = LECSVFormat.format.withHeader(headers);
                String line = reader.readLine(); // skip header
                for (; (line = reader.readLine()) != null; lineNum++) {
                    LOG.info("Start to processing line: " + lineNum);
                    try (CSVParser parser = CSVParser.parse(line, format)) {
                        beforeEachRecord();
                        CSVRecord csvRecord = parser.getRecords().get(0);
                        GenericRecord avroRecord = toGenericRecord(Sets.newHashSet(headers), csvRecord);
                        if (errorMap.size() == 0) {
                            dataFileWriter.append(avroRecord);
                            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
                        } else {
                            handleError(context, lineNum);
                        }
                    } catch (Exception e) {
                        LOG.warn(e);
                        rowError = true;
                        errorMap.put(String.valueOf(lineNum),
                                String.format(
                                        "%s, try to remove single quote \' or double quote \"  in the row and try again",
                                        e.getMessage()).toString());
                        handleError(context, lineNum);
                    }
                }
            }
        }
    }

    private void beforeEachRecord() {
        id = null;
        missingRequiredColValue = Boolean.FALSE;
        fieldMalFormed = Boolean.FALSE;
        rowError = Boolean.FALSE;
    }

    private void handleError(Context context, long lineNumber) throws IOException {
        if (missingRequiredColValue) {
            context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).increment(1);
        } else if (fieldMalFormed) {
            context.getCounter(RecordImportCounter.FIELD_MALFORMED).increment(1);
        } else if (rowError) {
            context.getCounter(RecordImportCounter.ROW_ERROR).increment(1);
        }
        context.getCounter(RecordImportCounter.IGNORED_RECORDS).increment(1);

        id = id != null ? id : "";
        csvFilePrinter.printRecord(lineNumber, id, errorMap.values().toString());
        csvFilePrinter.flush();

        errorMap.clear();
    }

    public String getCSVFilePath(URI[] uris) {
        for (URI uri : uris) {
            if (uri.getPath().endsWith(".csv")) {
                return new Path(uri.getPath()).getName();
            }
        }
        return null;
    }

    private GenericRecord toGenericRecord(Set<String> headers, CSVRecord csvRecord) {
        GenericRecord avroRecord = new GenericData.Record(schema);
        for (Attribute attr : table.getAttributes()) {
            Object avroFieldValue = null;
            if (headers.contains(attr.getDisplayName())) {
                Type avroType = schema.getField(attr.getName()).schema().getTypes().get(0).getType();
                String csvFieldValue = null;
                try {
                    csvFieldValue = String.valueOf(csvRecord.get(attr.getDisplayName()));
                } catch (Exception e) { // This catch is for the row error
                    rowError = true;
                    LOG.warn(e);
                }
                List<InputValidator> validators = attr.getValidators();
                try {
                    validateAttribute(validators, csvRecord, attr);
                    if (!attr.isNullable() || !StringUtils.isEmpty(csvFieldValue)) {
                        avroFieldValue = toAvro(csvFieldValue, avroType, attr);
                        if (attr.getName().equals(InterfaceName.Id.name())) {
                            id = String.valueOf(avroFieldValue);
                        }
                    }
                    avroRecord.put(attr.getName(), avroFieldValue);
                } catch (Exception e) {
                    LOG.warn(e);
                    errorMap.put(attr.getDisplayName(), e.getMessage());
                }
            } else {
                avroRecord.put(attr.getName(), avroFieldValue);
            }
        }
        avroRecord.put(InterfaceName.InternalId.name(), lineNum);
        return avroRecord;

    }

    private void validateAttribute(List<InputValidator> validators, CSVRecord csvRecord, Attribute attr) {
        String attrKey = attr.getName();
        if (!attr.isNullable() && StringUtils.isEmpty(csvRecord.get(attr.getDisplayName()))) {
            missingRequiredColValue = true;
            throw new RuntimeException(String.format("Required Column %s is missing value.", attr.getDisplayName()));
        }
        if (CollectionUtils.isNotEmpty(validators)) {
            InputValidator validator = validators.get(0);
            try {
                validator.validate(attrKey, csvRecord.toMap(), table);
            } catch (Exception e) {
                missingRequiredColValue = true;
                throw new RuntimeException(e.getMessage());
            }
        }
    }

    private Object toAvro(String fieldCsvValue, Type avroType, Attribute attr) {
        try {
            switch (avroType) {
            case DOUBLE:
                return new Double(parseStringToNumber(fieldCsvValue).doubleValue());
            case FLOAT:
                return new Float(parseStringToNumber(fieldCsvValue).floatValue());
            case INT:
                return new Integer(parseStringToNumber(fieldCsvValue).intValue());
            case LONG:
                if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
                    LOG.info("Date value from csv: " + fieldCsvValue);
                    return TimeStampConvertUtils.convertToLong(fieldCsvValue);
                } else {
                    return new Long(parseStringToNumber(fieldCsvValue).longValue());
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
                throw new IllegalArgumentException("Not supported Field, avroType: " + avroType + ", physicalDatalType:"
                        + attr.getPhysicalDataType());
            }
        } catch (IllegalArgumentException e) {
            fieldMalFormed = true;
            LOG.warn(e.getMessage());
            throw new RuntimeException(String.format("Cannot convert %s to type %s for column %s.", fieldCsvValue,
                    avroType, attr.getDisplayName()));
        } catch (Exception e) {
            fieldMalFormed = true;
            LOG.warn(e.getMessage());
            throw new RuntimeException(String.format("Cannot parse %s as %s for column %s.", fieldCsvValue,
                    attr.getPhysicalDataType(), attr.getDisplayName()));
        }
    }

    @VisibleForTesting
    Number parseStringToNumber(String inputStr) throws ParseException {
        NumberFormatter numberFormatter = new NumberFormatter();
        return numberFormatter.parse(inputStr, Locale.getDefault());
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
