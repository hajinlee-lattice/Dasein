package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
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
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.format.number.NumberStyleFormatter;
import org.springframework.retry.support.RetryTemplate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.mapreduce.counters.RecordImportCounter;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InputValidatorWrapper;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;

public class CSVImportMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {

    private static final Logger LOG = LoggerFactory.getLogger(CSVImportMapper.class);

    private static final String UNDERSCORE = "_";

    private static final String NULL = "null";

    private static final String SCIENTIFIC_REGEX = "^[-+]?[0-9]*\\.?[0-9]+([eE][-+]?[0-9]+)?$";
    private static final Pattern SCIENTIFIC_PTN = Pattern.compile(SCIENTIFIC_REGEX);
    private static final Set<String> VAL_TRUE = Sets.newHashSet("true", "t", "1", "yes", "y");
    private static final Set<String> VAL_FALSE = Sets.newHashSet("false", "f", "0", "no", "n");
    private static final Set<String> EMPTY_SET = Sets.newHashSet("none", "null", "na", "N/A", "Blank", "empty");
    private final BlockingQueue<RecordLine> recordQueue = new LinkedBlockingQueue<>(1000);

    private static final int MAX_STRING_LENGTH = 1000;

    private Schema schema;

    private Table table;

    private Path outputPath;

    private Configuration conf;

    private String idColumnName;

    private final String ERROR_FILE_NAME = "error";

    private final String AVRO_SUFFIX_NAME = ".avro";

    /**
     * RFC 4180 defines line breaks as CRLF
     */
    private final String CRLF = "\r\n";

    private LongAdder importedRecords = new LongAdder();

    private LongAdder ignoredRecords = new LongAdder();

    private LongAdder duplicateRecords = new LongAdder();

    private LongAdder requiredFieldMissing = new LongAdder();

    private LongAdder fieldMalformed = new LongAdder();

    private LongAdder rowErrorVal = new LongAdder();

    private Set<String> headers;

    private String avroFile;

    private ExecutorService service;

    private volatile boolean finishReading = false;

    private volatile boolean uploadErrorRecord = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LogManager.getLogger(CSVImportMapper.class).setLevel(Level.INFO);
        LogManager.getLogger(TimeStampConvertUtils.class).setLevel(Level.WARN);
        conf = context.getConfiguration();
        schema = AvroJob.getOutputKeySchema(conf);
        LOG.info("schema is: " + schema.toString());
        table = JsonUtils.deserialize(conf.get("eai.table.schema"), Table.class);
        LOG.info("table is:" + table);
        LOG.info("Deduplicate enable = false");
        idColumnName = conf.get("eai.id.column.name");
        LOG.info("Import file id column is: " + idColumnName);
        if (StringUtils.isEmpty(idColumnName)) {
            LOG.info("The id column does not exist.");
        }
        outputPath = MapFileOutputFormat.getOutputPath(context);
        LOG.info("Path is:" + outputPath);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        process(context);
        cleanup(context);
    }

    private String getFileName(String fileName, String fileType, int fileIndex) {
        StringBuffer stringBuffer = new StringBuffer();
        stringBuffer.append(fileName);
        stringBuffer.append(UNDERSCORE);
        stringBuffer.append(fileIndex);
        stringBuffer.append(fileType);
        return stringBuffer.toString();
    }

    private void handleProcess(int index) throws IOException {
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        String avroFileName = getFileName(avroFile, ".avro", index);
        boolean uploadAvroRecord = false;
        try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
            dataFileWriter.create(schema, new File(avroFileName));
            try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(getFileName("error", ".csv", index)),
                    LECSVFormat.format.withHeader((String[]) null))) {
                ConvertCSVToAvro convertCSVToAvro = new ConvertCSVToAvro(csvFilePrinter, dataFileWriter);
                while (true) {
                    try {
                        RecordLine recordLine = recordQueue.poll(10, TimeUnit.SECONDS);
                        if (recordLine != null) {
                            convertCSVToAvro.process(recordLine.csvRecord, recordLine.lineNum);
                        }
                        if (finishReading && recordQueue.isEmpty()) {
                            // only finish reading and queue is empty
                            LOG.info(String.format("finish parse thread, index is %d", index));
                            break;
                        }
                    } catch (InterruptedException e) {
                        LOG.info("Record queue was interrupted");
                    }
                }
                if (convertCSVToAvro.hasAvroRecord) {
                    uploadAvroRecord = true;
                }
                if (convertCSVToAvro.hasErrorRecord) {
                    uploadErrorRecord = true;
                }
            }
        }
        if (!uploadAvroRecord) {
            File file = new File(avroFileName);
            file.delete();
        }
    }

    private void process(Context context) throws IOException {
        String csvFileName = getCSVFilePath(context.getCacheFiles());
        if (csvFileName == null) {
            throw new RuntimeException("Not able to find csv file from localized files");
        }
        if (StringUtils.isEmpty(table.getName())) {
            avroFile = "file";
        } else {
            avroFile = table.getName();
        }
        long lineNum = 2;
        int cores = conf.getInt("mapreduce.map.cpu.vcores", 1);
        CSVFormat format = LECSVFormat.format.withFirstRecordAsHeader();
        try (CSVParser parser = new CSVParser(new BufferedReader(new InputStreamReader(
                new BOMInputStream(new FileInputStream(csvFileName), false, ByteOrderMark.UTF_8,
                        ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                        ByteOrderMark.UTF_32BE), StandardCharsets.UTF_8)), format)) {
            headers = Sets.newHashSet(new ArrayList<>(parser.getHeaderMap().keySet()).toArray(new String[]{}));
            Iterator<CSVRecord> iter = parser.iterator();
            String ERROR_FILE = getFileName("error", ".csv", 0);
            try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE),
                    LECSVFormat.format.withHeader((String[]) null))) {
                service = ThreadPoolUtils.getFixedSizeThreadPool("dataunit-mgr", cores);
                List<CompletableFuture<?>> futures = new ArrayList<>();
                for (int i = 1; i <= cores; i++) {
                    int index = i;
                    futures.add(CompletableFuture.runAsync(() -> {
                        try {
                            handleProcess(index);
                        } catch (IOException e) {
                            LOG.info(String.format("IOException %s happened when process csv record.", e.getMessage()));
                        }
                    }, service));
                }
                while (true) {
                    // capture IO exception produced during dealing with line
                    try {
                        CSVRecord csvRecord = iter.next();
                        recordQueue.put(new RecordLine(csvRecord, lineNum));
                        lineNum++;
                    } catch (IllegalStateException ex) {
                        LOG.warn(ex.getMessage(), ex);
                        rowErrorVal.increment();
                        ignoredRecords.increment();
                        Map<String, String> errorMap = new HashMap<>();
                        errorMap.put(String.valueOf(lineNum), String.format("CSV parser can't parse this line due to %s", ex.getMessage()));
                        csvFilePrinter.printRecord(lineNum, null, errorMap.values().toString());
                        csvFilePrinter.flush();
                        errorMap.clear();
                        uploadErrorRecord = true;
                    } catch (NoSuchElementException e) {
                        break;
                    }
                }
                finishReading = true;
                // wait for all the threads done
                CompletableFuture.allOf(futures.stream().toArray(CompletableFuture[]::new)).join();
                service.shutdown();
            }
        } catch (Exception e) {
            LOG.warn(e.getMessage(), e);
        }
    }

    public String getCSVFilePath(URI[] uris) {
        for (URI uri : uris) {
            if (uri.getPath().endsWith(".csv")) {
                return new Path(uri.getPath()).getName();
            }
        }
        return null;
    }

    private Boolean convertBooleanVal(String val) {
        if (VAL_TRUE.contains(val.toLowerCase())) {
            return Boolean.TRUE;
        } else if (VAL_FALSE.contains(val.toLowerCase())) {
            return Boolean.FALSE;
        } else {
            throw new IllegalArgumentException(String.format("Cannot parse %s as Boolean!", val));
        }
    }

    @VisibleForTesting
    void checkTimeZoneValidity(String fieldCsvValue, String timeZone) throws IllegalArgumentException {
        fieldCsvValue = fieldCsvValue.trim().replaceFirst("(\\s{2,})",
                TimeStampConvertUtils.SYSTEM_DELIMITER);
        if (StringUtils.isNotBlank(fieldCsvValue) && StringUtils.isNotBlank(timeZone)) {
            boolean isISO8601 = TimeStampConvertUtils.SYSTEM_USER_TIME_ZONE.equals(timeZone);
            boolean matchTZ = TimeStampConvertUtils.isIso8601TandZFromDateTime(fieldCsvValue);
            if (isISO8601 && !matchTZ) {
                // time zone is in ISO-8601, validate its value using T&Z
                throw new IllegalArgumentException("Time zone should be part of value but is not.");
            } else if (!isISO8601 && matchTZ) {
                // time zone is not in ISO-8601, validate value not using T&Z
                throw new IllegalArgumentException(String.format("Time zone set to %s. Value should not contain time " +
                        "zone setting.", timeZone));
            }
        }
    }

    @VisibleForTesting
    Number parseStringToNumber(String inputStr) throws ParseException, NullPointerException {
        inputStr = inputStr.trim();
        if (SCIENTIFIC_PTN.matcher(inputStr).matches()) {
            // handle scientific notation
            return Double.parseDouble(inputStr);
        }
        NumberStyleFormatter numberFormatter = new NumberStyleFormatter();
        return numberFormatter.parse(inputStr, Locale.getDefault());
    }

    @VisibleForTesting
    boolean isEmptyString(String inputStr) {
        inputStr = inputStr.trim();
        for (String emptyStr : EMPTY_SET) {
            if (emptyStr.equalsIgnoreCase(inputStr)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void cleanup(Context context) throws IOException {
        context.getCounter(RecordImportCounter.IMPORTED_RECORDS).setValue(importedRecords.longValue());
        context.getCounter(RecordImportCounter.IGNORED_RECORDS).setValue(ignoredRecords.longValue());
        context.getCounter(RecordImportCounter.DUPLICATE_RECORDS).setValue(duplicateRecords.longValue());
        context.getCounter(RecordImportCounter.REQUIRED_FIELD_MISSING).setValue(requiredFieldMissing.longValue());
        context.getCounter(RecordImportCounter.FIELD_MALFORMED).setValue(fieldMalformed.longValue());
        context.getCounter(RecordImportCounter.ROW_ERROR).setValue(rowErrorVal.longValue());
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
        uploadAvroFile(context);
        mergeErrorFile(context);
    }

    private void uploadAvroFile(Context context) throws IOException {
        File directory = new File(".");
        FilenameFilter filenameFilter = (file, name) -> name.endsWith(AVRO_SUFFIX_NAME);
        File[] avroFiles = directory.listFiles(filenameFilter);
        if (avroFiles != null) {
            for (File avroFile2 : avroFiles) {
                String avroFileName = avroFile2.getName();
                RetryTemplate retry = RetryUtils.getRetryTemplate(3);
                retry.execute(ctx -> {
                    if (ctx.getRetryCount() > 0) {
                        LOG.warn("Previous failure:", ctx.getLastThrowable());
                    }
                    String hdfsPath = outputPath + "/" + avroFileName;
                    if (HdfsUtils.fileExists(conf, hdfsPath)) {
                        HdfsUtils.rmdir(conf, hdfsPath);
                    }
                    HdfsUtils.copyLocalToHdfs(context.getConfiguration(), avroFileName, hdfsPath);
                    return null;
                });
            }
        }
    }

    private void mergeErrorFile(Context context) {
        if (uploadErrorRecord) {
            File directory = new File(".");
            FilenameFilter filenameFilter = (file, name) -> name.startsWith(ERROR_FILE_NAME);
            File[] errorFiles = directory.listFiles(filenameFilter);
            if (errorFiles != null) {
                File errorFileToUpload = new File(ImportProperty.ERROR_FILE);
                try {
                    if (!errorFileToUpload.exists()) {
                        errorFileToUpload.createNewFile();
                    }
                    try (FileOutputStream fileOutputStream = new FileOutputStream(errorFileToUpload)) {
                        fileOutputStream.write(StringUtils.join(ImportProperty.ERROR_HEADER, ",").getBytes());
                        fileOutputStream.write(CRLF.getBytes());
                        Arrays.asList(errorFiles).sort(File::compareTo);
                        for (File errorFile : errorFiles) {
                            try (FileInputStream inputStream = new FileInputStream(errorFile)) {
                                IOUtils.copy(inputStream, fileOutputStream);
                            }
                        }
                        HdfsUtils.copyLocalToHdfs(context.getConfiguration(), ImportProperty.ERROR_FILE, outputPath + "/" + ImportProperty.ERROR_FILE);
                    }
                } catch (IOException e) {
                    LOG.error(String.format("IOException happened during the process for merge file: %s.", e.getMessage()));
                }
            }
        }
    }

    private class RecordLine {

        RecordLine(CSVRecord csvRecord, long lineNum) {
            this.csvRecord = csvRecord;
            this.lineNum = lineNum;
        }

        private CSVRecord csvRecord;

        private long lineNum;
    }

    private class ConvertCSVToAvro {

        private Map<String, String> errorMap = new HashMap<>();

        private Map<String, String> duplicateMap = new HashMap<>();

        private CSVPrinter csvFilePrinter;

        private boolean missingRequiredColValue = Boolean.FALSE;

        private boolean fieldMalFormed = Boolean.FALSE;

        private boolean rowError = Boolean.FALSE;

        private GenericRecord avroRecord;

        private String id;

        private DataFileWriter<GenericRecord> dataFileWriter;

        private boolean hasAvroRecord = false;

        private boolean hasErrorRecord = false;

        ConvertCSVToAvro(CSVPrinter csvFilePrinter, DataFileWriter dataFileWriter) {
            this.avroRecord = new GenericData.Record(schema);
            this.csvFilePrinter = csvFilePrinter;
            this.dataFileWriter = dataFileWriter;
        }

        private void process(CSVRecord csvRecord, long lineNum) throws IOException {
            // capture IO exception produced during dealing with line
            beforeEachRecord();
            GenericRecord currentAvroRecord = toGenericRecord(csvRecord, lineNum);
            if (errorMap.size() == 0 && duplicateMap.size() == 0) {
                hasAvroRecord = true;
                dataFileWriter.append(currentAvroRecord);
                importedRecords.increment();
            } else {
                if (errorMap.size() > 0) {
                    handleError(lineNum);
                }
                if (duplicateMap.size() > 0) {
                    handleDuplicate(lineNum);
                }
            }
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
                    }
                }
            }
        }

        private GenericRecord toGenericRecord(CSVRecord csvRecord, long lineNum) {
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
                        LOG.warn(e.getMessage());
                    }
                    try {
                        if (StringUtils.isNotEmpty(csvFieldValue) && csvFieldValue.length() > MAX_STRING_LENGTH) {
                            throw new RuntimeException(String.format("%s exceeds %s chars", csvFieldValue, MAX_STRING_LENGTH));
                        }
                        validateAttribute(csvRecord, attr, csvColumnName);
                        if (StringUtils.isNotEmpty(attr.getDefaultValueStr()) || StringUtils.isNotEmpty(csvFieldValue)) {
                            if (StringUtils.isEmpty(csvFieldValue) && attr.getDefaultValueStr() != null) {
                                csvFieldValue = attr.getDefaultValueStr();
                            }
                            avroFieldValue = toAvro(csvFieldValue, avroType, attr, attr.getName().equals(idColumnName));
                            if (attr.getName().equals(idColumnName)) {
                                id = String.valueOf(avroFieldValue);
                                if (id.equalsIgnoreCase(NULL)) {
                                    throw new RuntimeException(String.format("The %s value is equals to string null", attr.getDisplayName()));
                                }
                            }
                        }
                        avroRecord.put(attr.getName(), avroFieldValue);
                    } catch (LedpException e) {
                        // Comment out warnings because log files are too large.
                        //LOG.warn(e.getMessage());
                        if (e.getCode().equals(LedpCode.LEDP_17017)) {
                            duplicateMap.put(attr.getDisplayName(), e.getMessage());
                        } else {
                            LOG.warn(e.getMessage());
                            throw e;
                        }
                    } catch (Exception e) {
                        // Comment out warnings because log files are too large.
                        //LOG.warn(e.getMessage());
                        errorMap.put(attr.getDisplayName(), e.getMessage());
                    }
                } else {
                    try {
                        validateAttribute(csvRecord, attr, csvColumnName);
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

        private Object toAvro(String fieldCsvValue, Type avroType, Attribute attr, boolean trimInput) {
            // Track a more descriptive Avro Type for error messages.
            String errorMsgAvroType = avroType.getName();
            try {
                if (trimInput && StringUtils.isNotEmpty(fieldCsvValue)) {
                    fieldCsvValue = fieldCsvValue.trim();
                }
                switch (avroType) {
                    case DOUBLE:
                        return new Double(parseStringToNumber(fieldCsvValue).doubleValue());
                    case FLOAT:
                        return new Float(parseStringToNumber(fieldCsvValue).floatValue());
                    case INT:
                        return new Integer(parseStringToNumber(fieldCsvValue).intValue());
                    case LONG:
                        if (isEmptyString(fieldCsvValue)) {
                            return null;
                        }
                        if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
                            errorMsgAvroType = "DATE";
                            // DP-11078 Add Time Zone Validation to Import Workflow
                            //　timezone is ISO 8601, value should be in T&Z format
                            checkTimeZoneValidity(fieldCsvValue, attr.getTimezone());
                            Long timestamp = TimeStampConvertUtils.convertToLong(fieldCsvValue, attr.getDateFormatString(),
                                    attr.getTimeFormatString(), attr.getTimezone());
                            if (timestamp < 0) {
                                // In order to support the requirements of:
                                //   https://solutions.lattice-engines.com/browse/PLS-12846  and
                                //   https://solutions.lattice-engines.com/browse/DP-9653
                                // We change the behavior of negative timestamps from throwing an exception and failing to
                                // parse the input CSV row to logging a warning and setting the timestamp value to zero.

                                // Comment out warnings because log files are too large.
                                //LOG.warn(String.format(
                                //        "Converting date/time %s to timestamp generated negative value %d for column %s",
                                //        fieldCsvValue, timestamp, attr.getDisplayName()));
                                return 0L;
                            }
                            return timestamp;
                        } else {
                            return new Long(parseStringToNumber(fieldCsvValue).longValue());
                        }
                    case STRING:
                        if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Timestamp)) {
                            errorMsgAvroType = "TIMESTAMP";
                            if (isEmptyString(fieldCsvValue)) {
                                return null;
                            }
                            if (fieldCsvValue.matches("[0-9]+")) {
                                return fieldCsvValue;
                            }
                            try {
                                Long timestamp = TimeStampConvertUtils.convertToLong(fieldCsvValue);
                                if (timestamp < 0) {
                                    throw new IllegalArgumentException("Cannot parse: " + fieldCsvValue +
                                            " using conversion library");
                                }
                                return Long.toString(timestamp);
                            } catch (Exception e) {
                                // Comment out warnings because log files are too large.
                                //LOG.warn(String.format("Error parsing date using TimeStampConvertUtils for column %s with " +
                                //        "value %s.", attr.getName(), fieldCsvValue));
                                DateTimeFormatter dtf = ISODateTimeFormat.dateTimeParser();
                                Long timestamp = dtf.parseDateTime(fieldCsvValue).getMillis();
                                if (timestamp < 0) {
                                    throw new IllegalArgumentException("Cannot parse: " + fieldCsvValue +
                                            " using conversion library or ISO 8601 Format");
                                }
                                return Long.toString(timestamp);
                            }
                        }
                        return fieldCsvValue;
                    case ENUM:
                        return fieldCsvValue;
                    case BOOLEAN:
                        return convertBooleanVal(fieldCsvValue);
                    default:
                        // Comment out warnings because log files are too large.
                        //LOG.info("size is:" + fieldCsvValue.length());
                        throw new IllegalArgumentException("Not supported Field, avroType: " + avroType + ", physicalDataType:"
                                + attr.getPhysicalDataType());
                }
            } catch (IllegalArgumentException e) {
                fieldMalFormed = true;
                // Comment out warnings because log files are too large.
                //LOG.warn(e.getMessage());
                throw new RuntimeException(String.format("Cannot convert %s to type %s for column %s.\n" +
                        "Error message was: %s", fieldCsvValue, errorMsgAvroType, attr.getDisplayName(), e.getMessage()),
                        e);
            } catch (Exception e) {
                fieldMalFormed = true;
                // Comment out warnings because log files are too large.
                //LOG.warn(e.getMessage());
                throw new RuntimeException(String.format("Cannot parse %s as %s for column %s.\n" +
                                "Error message was: %s", fieldCsvValue, attr.getPhysicalDataType(), attr.getDisplayName(),
                        e.toString()), e);
            }
        }

        private void handleError(long lineNumber) throws IOException {
            if (missingRequiredColValue) {
                requiredFieldMissing.increment();
            } else if (fieldMalFormed) {
                fieldMalformed.increment();
            } else if (rowError) {
                rowErrorVal.increment();
            }
            ignoredRecords.increment();
            hasErrorRecord = true;
            id = id != null ? id : "";
            csvFilePrinter.printRecord(lineNumber, id, errorMap.values().toString());
            csvFilePrinter.flush();
            errorMap.clear();
        }

        private void handleDuplicate(long lineNumber) throws IOException {
            LOG.info("Handle duplicate record in line: " + lineNumber);
            duplicateRecords.increment();
            id = id != null ? id : "";
            csvFilePrinter.printRecord(lineNumber, id, duplicateMap.values().toString());
            csvFilePrinter.flush();
            duplicateMap.clear();
            hasErrorRecord = true;
        }

        public void beforeEachRecord() {
            id = null;
            missingRequiredColValue = Boolean.FALSE;
            fieldMalFormed = Boolean.FALSE;
            rowError = Boolean.FALSE;
            for (int i = 0; i < schema.getFields().size(); i++) {
                avroRecord.put(i, null);
            }
        }
    }

}
