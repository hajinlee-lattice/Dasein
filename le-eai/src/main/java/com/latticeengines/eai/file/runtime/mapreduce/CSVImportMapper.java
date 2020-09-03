package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
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
import java.util.stream.Collectors;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.ArchiveInputStream;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.ByteOrderMark;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableLong;
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

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.CompressionUtils;
import com.latticeengines.common.exposed.util.CompressionUtils.CompressType;
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

    private static final String CSV_RECORD_ERROR = "CSV_Record_Error";
    private static final String ALL_FIELDS_EMPTY = "All fields are empty!";
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

    private Map<String, Integer> headerMap;

    private String avroFile;

    private ExecutorService service;

    private AmazonS3 s3Client;

    private boolean useS3Input;

    private volatile boolean finishReading = false;

    private volatile boolean uploadErrorRecord = false;

    private boolean detailError = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        LogManager.getLogger(CSVImportMapper.class).setLevel(Level.INFO);
        LogManager.getLogger(TimeStampConvertUtils.class).setLevel(Level.WARN);

        conf = context.getConfiguration();
        schema = AvroJob.getOutputKeySchema(conf);
        LOG.info("schema is: " + schema.toString());
        table = JsonUtils.deserialize(conf.get("eai.table.schema"), Table.class);
        LOG.info("table is:" + table);
        idColumnName = conf.get("eai.id.column.name");
        LOG.info("Import file id column is: " + idColumnName);
        if (StringUtils.isEmpty(idColumnName)) {
            LOG.info("The id column does not exist.");
        }
        outputPath = MapFileOutputFormat.getOutputPath(context);
        LOG.info("Path is:" + outputPath);
        detailError = conf.getBoolean("eai.import.detail.error", false);
        LOG.info("Detail Error: " + detailError);
        useS3Input = conf.getBoolean("eai.import.use.s3.input", false);
        if (useS3Input) {
            String region = conf.get("eai.import.aws.region");
            String awsKey = CipherUtils.decrypt(conf.get("eai.import.aws.access.key")).replace("\n", "");
            String awsSecret = CipherUtils.decrypt(conf.get("eai.import.aws.secret.key")).replace("\n", "");
            s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(awsKey, awsSecret)))
                    .withRegion(Regions.fromName(region))
                    .build();
        }
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);
        process(context);
        cleanup(context);
    }

    private String getFileName(String fileName, String fileType, int fileIndex) {
        return fileName + UNDERSCORE + fileIndex + fileType;
    }

    private void handleProcess(int index) throws IOException {
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        String avroFileName = getFileName(avroFile, ".avro", index);
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
                if (convertCSVToAvro.hasErrorRecord) {
                    uploadErrorRecord = true;
                }
            }
        }
    }

    private InputStream getInputStreamFromS3() {
        String s3Bucket = conf.get("eai.import.aws.s3.bucket");
        String objectKey = sanitizePathToKey(conf.get("eai.import.aws.s3.object.key"));
        if (s3Client.doesObjectExist(s3Bucket, objectKey)) {
            GetObjectRequest getObjectRequest = new GetObjectRequest(s3Bucket, objectKey);
            try {
                S3Object s3Object = s3Client.getObject(getObjectRequest);
                LOG.info(String.format("Reading the object %s of type %s and size %s", objectKey,
                        s3Object.getObjectMetadata().getContentType(),
                        FileUtils.byteCountToDisplaySize(s3Object.getObjectMetadata().getContentLength())));
                return s3Object.getObjectContent();
            } catch (AmazonS3Exception e) {
                throw new RuntimeException("Failed to get object " + objectKey + " from S3 bucket " + s3Bucket, e);
            }
        } else {
            LOG.error("Object " + objectKey + " does not exist in bucket " + s3Bucket);
            throw new RuntimeException("Not able to find csv file from s3!");
        }
    }

    private InputStream getInputFileStream(Context context) throws IOException {
        if (useS3Input) {
            return getInputStreamFromS3();
        } else {
            String csvFileName = getCSVFilePath(context.getCacheFiles());
            if (csvFileName == null) {
                throw new RuntimeException("Not able to find csv file from localized files");
            }
            return new FileInputStream(csvFileName);
        }
    }

    private String sanitizePathToKey(String path) {
        while (path.startsWith("/")) {
            path = path.substring(1);
        }
        while (path.endsWith("/")) {
            path = path.substring(0, path.lastIndexOf("/"));
        }
        return path;
    }

    private CompressType getCompressType() {
        CompressType compressType = CompressType.NO_COMPRESSION;
        if (useS3Input) {
            compressType = CompressionUtils.getCompressType(getInputStreamFromS3());
        }
        return compressType;
    }

    private List<CompletableFuture<?>> initFutures (int cores){
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
        return futures;
    }

    private void process(Context context) {
        if (StringUtils.isEmpty(table.getName())) {
            avroFile = "file";
        } else {
            avroFile = table.getName();
        }
        MutableLong lineNum = new MutableLong(2);
        int cores = conf.getInt("mapreduce.map.cpu.vcores", 1);
        service = ThreadPoolUtils.getFixedSizeThreadPool("dataunit-mgr", cores);
        CSVFormat format = LECSVFormat.format.withFirstRecordAsHeader();
        List<CompletableFuture<?>> futures = initFutures(cores);
        try {
            CompressType compressType = getCompressType();
            LOG.info("compress type is: " + compressType);
            idColumnName = conf.get("eai.id.column.name");
            try (InputStream inputStream = CompressionUtils.getCompressInputStream(new BOMInputStream(getInputFileStream(context), false,
                    ByteOrderMark.UTF_8, ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                    ByteOrderMark.UTF_32BE), compressType)) {
                if (inputStream instanceof ArchiveInputStream) {
                    ArchiveEntry archiveEntry;
                    ArchiveInputStream archiveInputStream = (ArchiveInputStream) inputStream;
                    while ((archiveEntry = archiveInputStream.getNextEntry()) != null) {
                        if (CompressionUtils.isValidArchiveEntry(archiveEntry)) {
                            parseCSV(inputStream, format, lineNum);
                        }
                    }
                } else {
                    parseCSV(inputStream, format, lineNum);
                }
            }
        } catch (Exception e) {
            LOG.warn("Can't process csv file: {}.", e.getMessage());
        } finally {
            finishReading = true;
            // wait for all the threads done
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            service.shutdown();
        }
    }

    private void parseCSV(InputStream inputStream, CSVFormat format, MutableLong lineNum) {
        try {
            CSVParser parser = new CSVParser(new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)), format);
            if (MapUtils.isEmpty(headerMap)) {
                headerMap = parser.getHeaderMap();
            }
            Iterator<CSVRecord> iter = parser.iterator();
            String ERROR_FILE = getFileName("error", ".csv", 0);
            try (CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE),
                    LECSVFormat.format.withHeader((String[]) null))) {
                while (true) {
                    // capture IO exception produced during dealing with line
                    try {
                        CSVRecord csvRecord = iter.next();
                        recordQueue.put(new RecordLine(csvRecord, lineNum.getValue()));
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
                    lineNum.increment();
                }
            }
        } catch (Exception e) {
            LOG.warn("Exception happened during the process of parsing CSV file: ", e);
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
            for (File avroFile : avroFiles) {
                String avroFileName = avroFile.getName();
                RetryTemplate retry = RetryUtils.getRetryTemplate(3);
                retry.execute(ctx -> {
                    if (ctx.getRetryCount() > 0) {
                        LOG.warn("Previous failure:", ctx.getLastThrowable());
                    }
                    String hdfsPath = outputPath + "/" + avroFileName;
                    if (HdfsUtils.fileExists(conf, hdfsPath)) {
                        HdfsUtils.rmdir(conf, hdfsPath);
                    }
                    if (hasRecord(avroFile)) {
                        HdfsUtils.copyLocalToHdfs(context.getConfiguration(), avroFileName, hdfsPath);
                    }
                    return null;
                });
            }
        }
    }

    private boolean hasRecord(File avroFile) throws IOException {
        try (DataFileStream<GenericRecord> reader = new DataFileStream<>(new FileInputStream(avroFile),
                new GenericDatumReader<>())) {
            return reader.hasNext();
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
                        if (!errorFileToUpload.createNewFile()) {
                            LOG.error("Cannot create error log file!");
                        }
                    }
                    try (FileOutputStream fileOutputStream = new FileOutputStream(errorFileToUpload)) {
                        fileOutputStream.write(getErrorFileHeader().getBytes());
                        fileOutputStream.write(CRLF.getBytes());
                        Arrays.sort(errorFiles, File::compareTo);
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

    private String getErrorFileHeader() {
        if (detailError) {
            List<Map.Entry<String, Integer>> headerLists = new LinkedList<>(headerMap.entrySet());
            headerLists.sort(Map.Entry.comparingByValue());
            List<String> headers = new ArrayList<>();
            headerLists.forEach(header -> headers.add(header.getKey()));
            Collections.addAll(headers, ImportProperty.ERROR_HEADER);
            return StringUtils.join(headers, ",");
        } else {
            return StringUtils.join(ImportProperty.ERROR_HEADER, ",");
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

        private boolean hasErrorRecord = false;

        ConvertCSVToAvro(CSVPrinter csvFilePrinter, DataFileWriter<GenericRecord> dataFileWriter) {
            this.avroRecord = new GenericData.Record(schema);
            this.csvFilePrinter = csvFilePrinter;
            this.dataFileWriter = dataFileWriter;
        }

        private void process(CSVRecord csvRecord, long lineNum) throws IOException {
            // capture IO exception produced during dealing with line
            beforeEachRecord();
            if (checkCSVRecord(csvRecord)) {
                GenericRecord currentAvroRecord = toGenericRecord(csvRecord, lineNum);
                if (errorMap.size() == 0 && duplicateMap.size() == 0) {
                    dataFileWriter.append(currentAvroRecord);
                    importedRecords.increment();
                } else {
                    if (errorMap.size() > 0) {
                        handleError(lineNum, csvRecord);
                    }
                    if (duplicateMap.size() > 0) {
                        handleDuplicate(lineNum);
                    }
                }
            } else {
                if (errorMap.size() > 0) {
                    handleError(lineNum, csvRecord);
                }
            }
        }

        private boolean checkCSVRecord(CSVRecord csvRecord) {
            // 1. csv record itself cannot be null.
            if (csvRecord == null) {
                return false;
            }
            // 2. not all values are empty
            for (String value: csvRecord) {
                if (StringUtils.isNotEmpty(value)) {
                    return true;
                }
            }
            errorMap.put(CSV_RECORD_ERROR, ALL_FIELDS_EMPTY);
            return false;
        }

        private void validateAttribute(CSVRecord csvRecord, Attribute attr,
                                       Map<String, String> headerCaseMapping, String csvColumnNameInLowerCase) {
            String attrKey = attr.getName();
            String csvColumnName = headerCaseMapping.get(csvColumnNameInLowerCase);
            if (!attr.isNullable() && attr.getDefaultValueStr() == null) {
                if (StringUtils.isBlank(csvColumnName)) {
                    throw new RuntimeException(String.format("csv file should contain the column %s", csvColumnNameInLowerCase));
                }
                if (StringUtils.isEmpty(csvRecord.get(csvColumnName))) {
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
            Map<String, String> headerCaseMapping = headerMap.keySet().stream()
                                                    .collect(Collectors.toMap(String::toLowerCase, header -> header));
            for (Attribute attr : table.getAttributes()) {
                Object avroFieldValue = null;
                String csvColumnNameInLowerCase = attr.getSourceAttrName() == null ?
                        attr.getDisplayName().toLowerCase() : attr.getSourceAttrName().toLowerCase();
                // try other possible names:
                if (!headerCaseMapping.containsKey(csvColumnNameInLowerCase)) {
                    List<String> possibleNames = attr.getPossibleCSVNames();
                    if (CollectionUtils.isNotEmpty(possibleNames)) {
                        for (String possibleName : possibleNames) {
                            if (headerCaseMapping.containsKey(possibleName.toLowerCase())) {
                                csvColumnNameInLowerCase = possibleName.toLowerCase();
                                break;
                            }
                        }
                    }
                }
                if (headerCaseMapping.containsKey(csvColumnNameInLowerCase) || attr.getDefaultValueStr() != null) {
                    Type avroType = schema.getField(attr.getName()).schema().getTypes().get(0).getType();
                    String csvFieldValue = null;
                    try {
                        if (headerCaseMapping.containsKey(csvColumnNameInLowerCase)) {
                            csvFieldValue = String.valueOf(csvRecord.get(headerCaseMapping.get(csvColumnNameInLowerCase)));
                        }
                    } catch (Exception e) { // This catch is for the row error
                        rowError = true;
                        LOG.warn(e.getMessage());
                    }
                    try {
                        if (StringUtils.length(csvFieldValue) > MAX_STRING_LENGTH) {
                            throw new RuntimeException(String.format("%s exceeds %s chars", csvFieldValue, MAX_STRING_LENGTH));
                        }
                        validateAttribute(csvRecord, attr, headerCaseMapping, csvColumnNameInLowerCase);
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
                        validateAttribute(csvRecord, attr, headerCaseMapping, csvColumnNameInLowerCase);
                    } catch (Exception e) {
                        LOG.warn(e.getMessage());
                        errorMap.put(attr.getDisplayName(), e.getMessage());
                    }
                    if (attr.getRequired() || !attr.isNullable()) {
                        String oldMsg = errorMap.get(attr.getName());
                        String newMsg = String.format("%s cannot be empty! Expecting value from CSV column \"%s\".",
                                attr.getName(),
                                attr.getSourceAttrName() == null ? attr.getDisplayName() : attr.getSourceAttrName());
                        String msg = StringUtils.isBlank(oldMsg) ? newMsg : String.format("%s,%s", oldMsg, newMsg);
                        errorMap.put(attr.getName(), msg);
                    } else {
                        avroRecord.put(attr.getName(), avroFieldValue);
                    }
                }
            }
            avroRecord.put(InterfaceName.InternalId.name(), getInternalIdObj(lineNum));
            return avroRecord;
        }

        private Object getInternalIdObj(long lineNum) {
            if (table.getAttribute(InterfaceName.InternalId.name()) != null) {
                Attribute attr = table.getAttribute(InterfaceName.InternalId.name());
                Type avroType = schema.getField(attr.getName()).schema().getTypes().get(0).getType();
                return toAvro(String.valueOf(lineNum), avroType, attr, true);
            } else {
                return lineNum;
            }
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
                        return parseStringToNumber(fieldCsvValue).doubleValue();
                    case FLOAT:
                        return parseStringToNumber(fieldCsvValue).floatValue();
                    case INT:
                        return parseStringToNumber(fieldCsvValue).intValue();
                    case LONG:
                        if (isEmptyString(fieldCsvValue)) {
                            return null;
                        }
                        if (attr.getLogicalDataType() != null && attr.getLogicalDataType().equals(LogicalDataType.Date)) {
                            errorMsgAvroType = "DATE";
                            // DP-11078 Add Time Zone Validation to Import Workflow
                            //　timezone is ISO 8601, value should be in T&Z format
                            checkTimeZoneValidity(fieldCsvValue, attr.getTimezone());
                            long timestamp = TimeStampConvertUtils.convertToLong(fieldCsvValue, attr.getDateFormatString(),
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
                            return parseStringToNumber(fieldCsvValue).longValue();
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
                                long timestamp = TimeStampConvertUtils.convertToLong(fieldCsvValue);
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
                                long timestamp = dtf.parseDateTime(fieldCsvValue).getMillis();
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

        private void handleError(long lineNumber, CSVRecord csvRecord) throws IOException {
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
            List<String> errorDetail  = new ArrayList<>();
            if (detailError) {
                for (String s : csvRecord) {
                    errorDetail.add(s);
                }
            }
            errorDetail.add(String.valueOf(lineNumber));
            errorDetail.add(id);
            errorDetail.add(String.valueOf(errorMap.values().toString()));
            csvFilePrinter.printRecord(errorDetail);
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
