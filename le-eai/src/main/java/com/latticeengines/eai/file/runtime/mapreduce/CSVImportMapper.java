package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.number.NumberStyleFormatter;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.exception.CriticalImportException;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.validators.FailImportIfFieldIsEmpty;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;

public class CSVImportMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(CSVImportMapper.class);

    private static final String ERROR_FILE = "error.csv";

    private static final String NULL = "null";

    private static final String SCIENTIFIC_REGEX = "^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$";
    private static final Pattern SCIENTIFIC_PTN = Pattern.compile(SCIENTIFIC_REGEX);

    private Schema schema;

    private Table table;

    private Path outputPath;

    private Configuration conf;

    private boolean deduplicate;

    private boolean missingRequiredColValue = Boolean.FALSE;
    private boolean fieldMalFormed = Boolean.FALSE;
    private boolean rowError = Boolean.FALSE;

    private Map<String, String> errorMap = new HashMap<>();

    private Map<String, String> duplicateMap = new HashMap<>();

    private Set<String> uniqueIds = new HashSet<>();

    private CSVPrinter csvFilePrinter;

    private String idColumnName;

    private long lineNum = 2;

    private String avroFileName;

    private String id;

    private boolean failMapper = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        conf = context.getConfiguration();
        schema = AvroJob.getOutputKeySchema(conf);
        LOG.info("schema is: " + schema.toString());

        table = JsonUtils.deserialize(conf.get("eai.table.schema"), Table.class);
        LOG.info("table is:" + table);

        deduplicate = Boolean.parseBoolean(conf.get("eai.dedup.enable"));
        LOG.info("Deduplicate enable = " + String.valueOf(deduplicate));

        idColumnName = conf.get("eai.id.column.name");
        LOG.info("Import file id column is: " + idColumnName);

        if (StringUtils.isEmpty(idColumnName)) {
            LOG.info("The id column does not exist.");
            deduplicate = false;
        }

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
        try (CSVParser parser = new CSVParser(
                new InputStreamReader((new FileInputStream(csvFileName)), StandardCharsets.UTF_8),
                LECSVFormat.format)) {
            headers = new ArrayList<String>(parser.getHeaderMap().keySet()).toArray(new String[] {});
        }
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(schema, new File(avroFileName));
            CSVFormat format = LECSVFormat.format.withHeader(headers).withFirstRecordAsHeader();
            try (CSVParser parser = new CSVParser(
                    new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName), StandardCharsets.UTF_8)),
                    format)) {
                Iterator<CSVRecord> iter = parser.iterator();
                while (true) {
                    // capture IO exception produced during dealing with line
                    try {
                        if (failMapper) {
                            throw new CriticalImportException(
                                    "There's critical exception in import, will fail the job!");
                        }
                        LOG.debug("Start to processing line: " + lineNum);
                        beforeEachRecord();
                        GenericRecord avroRecord = toGenericRecord(Sets.newHashSet(headers), iter.next());
                        if (errorMap.size() == 0 && duplicateMap.size() == 0) {
                            dataFileWriter.append(avroRecord);
                            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).increment(1);
                        } else {
                            if (errorMap.size() > 0) {
                                handleError(context, lineNum);
                            }
                            if (duplicateMap.size() > 0) {
                                handleDuplicate(context, lineNum);
                            }
                        }
                        lineNum++;
                    } catch (IllegalStateException ex) {
                        LOG.warn(ex.getMessage(), ex);
                        rowError = true;
                        errorMap.put(String.valueOf(lineNum), String.format(
                                "%s, try to remove single quote \' or double quote \"  in the row and try again",
                                ex.getMessage()).toString());
                        handleError(context, lineNum);
                    } catch (NoSuchElementException e) {
                        break;
                    }
                }
            } catch (CriticalImportException critical) {
                throw critical;
            } catch (Exception e) {
                LOG.warn(e.getMessage(), e);
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

    private void handleDuplicate(Context context, long lineNumber) throws IOException {
        LOG.info("Handle duplicate record in line: " + String.valueOf(lineNumber));
        context.getCounter(RecordImportCounter.DUPLICATE_RECORDS).increment(1);
        id = id != null ? id : "";
        csvFilePrinter.printRecord(lineNumber, id, duplicateMap.values().toString());
        csvFilePrinter.flush();

        duplicateMap.clear();
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
            String csvColumnName = attr.getDisplayName();
            // try other possible names:
            if (!headers.contains(csvColumnName)) {
                List<String> possibleNames = attr.getPossibleCSVNames();
                if (CollectionUtils.isNotEmpty(possibleNames)) {
                    for (String possibleName : possibleNames) {
                        if (headers.contains(possibleName)) {
                            csvColumnName = possibleName;
                            break;
                        }
                    }
                }
            }
            if (headers.contains(csvColumnName) || attr.getDefaultValueStr() != null) {
                Type avroType = schema.getField(attr.getName()).schema().getTypes().get(0).getType();
                String csvFieldValue = null;
                try {
                    if (headers.contains(csvColumnName)) {
                        csvFieldValue = String.valueOf(csvRecord.get(csvColumnName));
                    }
                } catch (Exception e) { // This catch is for the row error
                    rowError = true;
                    LOG.warn(e.getMessage(), e);
                }
                try {
                    validateAttribute(csvRecord, attr, csvColumnName);
                    if (StringUtils.isNotEmpty(attr.getDefaultValueStr()) || StringUtils.isNotEmpty(csvFieldValue)) {
                        if (StringUtils.isEmpty(csvFieldValue) && attr.getDefaultValueStr() != null) {
                            csvFieldValue = attr.getDefaultValueStr();
                        }
                        avroFieldValue = toAvro(csvFieldValue, avroType, attr);
                        if (attr.getName().equals(idColumnName)) {
                            id = String.valueOf(avroFieldValue);
                            if (id.equalsIgnoreCase(NULL)) {
                                throw new RuntimeException(String.format("The %s value is equals to string null", attr.getDisplayName()));
                            }
                            if (deduplicate) {
                                if (uniqueIds.contains(id)) {
                                    throw new LedpException(LedpCode.LEDP_17017, new String[]{id});
                                } else {
                                    uniqueIds.add(id);
                                }
                            }
                        }
                    }
                    avroRecord.put(attr.getName(), avroFieldValue);
                } catch(CriticalImportException e) {
                    LOG.error(String.format("Import will fail because of critical exception %s", e.getMessage()));
                    errorMap.put(attr.getDisplayName(), e.getMessage());
                    failMapper = true;
                } catch (LedpException e) {
                    LOG.warn(e.getMessage());
                    if (e.getCode().equals(LedpCode.LEDP_17017)) {
                        duplicateMap.put(attr.getDisplayName(), e.getMessage());
                    } else {
                        throw e;
                    }
                } catch (Exception e) {
                    LOG.warn(e.getMessage());
                    errorMap.put(attr.getDisplayName(), e.getMessage());
                }
            } else {
                try {
                    validateAttribute(csvRecord, attr, csvColumnName);
                } catch (CriticalImportException e) {
                    LOG.error(String.format("Import will fail because of critical exception %s", e.getMessage()));
                    errorMap.put(attr.getDisplayName(), e.getMessage());
                    failMapper = true;
                } catch (Exception e) {
                    LOG.warn(e.getMessage());
                    errorMap.put(attr.getDisplayName(), e.getMessage());
                }
                if (attr.getRequired() || !attr.isNullable()) {
                    errorMap.put(attr.getName(), String.format("%s cannot be empty!", attr.getName()));
                } else {
                    avroRecord.put(attr.getName(), avroFieldValue);
                }
            }
        }
        avroRecord.put(InterfaceName.InternalId.name(), lineNum);
        return avroRecord;

    }

    private void validateAttribute(CSVRecord csvRecord, Attribute attr, String csvColumnName) {
        String attrKey = attr.getName();
        if (!attr.isNullable() && StringUtils.isEmpty(csvRecord.get(csvColumnName))) {
            if (attr.getDefaultValueStr() == null) {
                missingRequiredColValue = true;
                throw new RuntimeException(String.format("Required Column %s is missing value.", attr.getDisplayName()));
            }
        }
        List<InputValidatorWrapper> validatorWrappers = attr.getValidatorWrappers();
        if (CollectionUtils.isNotEmpty(validatorWrappers)) {
            for (InputValidatorWrapper validatorWrapper : validatorWrappers) {
                if (validatorWrapper.getType().equals(RequiredIfOtherFieldIsEmpty.class)) {
                    try {
                        validatorWrapper.getValidator().validate(attrKey, csvRecord.toMap(), table);
                    } catch (Exception e) {
                        missingRequiredColValue = true;
                        throw new RuntimeException(e.getMessage());
                    }
                } else if (validatorWrapper.getType().equals(FailImportIfFieldIsEmpty.class)) {
                    if (!validatorWrapper.getValidator().validate(attrKey, csvRecord.toMap(), table)) {
                        throw new CriticalImportException(String.format("Field %s is empty from import file!", attrKey));
                    }
                }
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
                    Long timestamp = TimeStampConvertUtils.convertToLong(fieldCsvValue, attr.getDateFormatString(),
                            attr.getTimeFormatString(), attr.getTimezone());
                    if (timestamp < 0) {
                        throw new IllegalArgumentException("Cannot parse: " + fieldCsvValue);
                    }
                    return timestamp;
                } else {
                    return new Long(parseStringToNumber(fieldCsvValue).longValue());
                }
            case STRING:
                if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Timestamp)) {
                    if (fieldCsvValue.matches("[0-9]+")) {
                        return fieldCsvValue;
                    }
                    try {
                        Long timestamp = TimeStampConvertUtils.convertToLong(fieldCsvValue);
                        if (timestamp < 0) {
                            throw new IllegalArgumentException("Cannot parse: " + fieldCsvValue);
                        }
                        return Long.toString(timestamp);
                    } catch (Exception e) {
                        LOG.warn(String.format("Error parsing date using TimeStampConvertUtils for column %s with " +
                                "value %s.", attr.getName(), fieldCsvValue));
                        DateTimeFormatter dtf = ISODateTimeFormat.dateTimeParser();
                        Long timestamp = dtf.parseDateTime(fieldCsvValue).getMillis();
                        if (timestamp < 0) {
                            throw new IllegalArgumentException("Cannot parse: " + fieldCsvValue);
                        }
                        return Long.toString(timestamp);
                    }
                }
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
            throw new RuntimeException(String.format("Cannot convert %s to type %s for column %s.\n" +
                    "Error message was: %s", fieldCsvValue, avroType, attr.getDisplayName(), e.getMessage()), e);
        } catch (Exception e) {
            fieldMalFormed = true;
            LOG.warn(e.getMessage());
            throw new RuntimeException(String.format("Cannot parse %s as %s for column %s.\n" +
                    "Error message was: %s", fieldCsvValue, attr.getPhysicalDataType(), attr.getDisplayName(),
                    e.getMessage()), e);
        }
    }

    @VisibleForTesting
    Number parseStringToNumber(String inputStr) throws ParseException,NullPointerException {
        inputStr = inputStr.trim();
        if (SCIENTIFIC_PTN.matcher(inputStr).matches()) {
            // handle scientific notation
            return Double.parseDouble(inputStr);
        }
        NumberStyleFormatter numberFormatter = new NumberStyleFormatter();
        return numberFormatter.parse(inputStr, Locale.getDefault());
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        csvFilePrinter.close();
        HdfsUtils.copyLocalToHdfs(context.getConfiguration(), avroFileName, outputPath + "/" + avroFileName);
        if (context.getCounter(RecordImportCounter.IMPORTED_RECORDS).getValue() == 0) {
            context.getCounter(RecordImportCounter.IMPORTED_RECORDS).setValue(0);
        }

        if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() == 0
                && context.getCounter(RecordImportCounter.DUPLICATE_RECORDS).getValue() == 0) {
            context.getCounter(RecordImportCounter.IGNORED_RECORDS).setValue(0);
            context.getCounter(RecordImportCounter.DUPLICATE_RECORDS).setValue(0);
        } else {
            if (context.getCounter(RecordImportCounter.DUPLICATE_RECORDS).getValue() == 0) {
                context.getCounter(RecordImportCounter.DUPLICATE_RECORDS).setValue(0);
            }
            if (context.getCounter(RecordImportCounter.IGNORED_RECORDS).getValue() == 0) {
                context.getCounter(RecordImportCounter.IGNORED_RECORDS).setValue(0);
            }
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
