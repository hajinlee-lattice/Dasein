package com.latticeengines.eai.file.runtime.mapreduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
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
import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.MapFileOutputFormat;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
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
    private static final Set<String> EMPTY_SET = Sets.newHashSet("none", "null", "na", "N/A", "Blank",
            "empty");

    private static final int MAX_STRING_LENGTH = 1000;

    private Schema schema;

    private Table table;

    private Path outputPath;

    private Configuration conf;

    private String idColumnName;

    private String id;

    private long gigaByte = 1073741824l;

    private long csvFilekSize;

    private final String ERROR_FILE_NAME = "error";

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
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        csvFilekSize = fileSplit.getLength();
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

    private void process(Context context) throws IOException {
        String csvFileName = getCSVFilePath(context.getCacheFiles());
        if (csvFileName == null) {
            throw new RuntimeException("Not able to find csv file from localized files");
        }
        String avroFile;
        if (StringUtils.isEmpty(table.getName())) {
            avroFile = "file";
        } else {
            avroFile = table.getName();
        }
        if (csvFilekSize <= gigaByte) {
            String ERROR_FILE = ImportProperty.ERROR_FILE;
            CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE),
                    LECSVFormat.format.withHeader(ImportProperty.ERROR_HEADER));
            String avroFileName = getFileName(avroFile, ".avro", 0);
            ConvertCSVToAvro convertCSVToAvro = new ConvertCSVToAvro(0, csvFilekSize, csvFileName, avroFileName, ERROR_FILE, csvFilePrinter, context);
            convertCSVToAvro.process();
        } else {
            int cores = conf.getInt("mapreduce.map.cpu.vcores", 1);
            LOG.info(String.format("CPU cores for import mapper is %d", cores));
            long splitSize = csvFilekSize / cores;
            ExecutorService service = ThreadPoolUtils.getFixedSizeThreadPool("dataunit-mgr", cores);
            int index = 0;
            List<Triple<Integer, Long, Long>> splits = new ArrayList<>();
            while (index < cores) {
                long start = index * splitSize;
                if (index == cores - 1) {
                    splits.add(new ImmutableTriple<>(index, start, csvFilekSize - start));
                } else {
                    splits.add(new ImmutableTriple<>(index, start, splitSize));
                }
                index++;
            }
            // need to use multi thread to do it
            CompletableFuture[] results = splits.stream().map(split -> CompletableFuture
                    .runAsync(() -> handleProcess(split, avroFile, csvFileName, context), service)).toArray(CompletableFuture[]::new);
            CompletableFuture.allOf(results).join();
        }
    }

    private void handleProcess(Triple<Integer, Long, Long> split, String avroFile, String csvFileName, Context context) {
        try {
            String ERROR_FILE = getFileName("error", ".csv", split.getLeft());
            CSVPrinter csvFilePrinter = new CSVPrinter(new FileWriter(ERROR_FILE),
                    LECSVFormat.format.withHeader((String[]) null));
            String avroFileName = getFileName(avroFile, ".avro", split.getLeft());
            ConvertCSVToAvro convertCSVToAvro = new ConvertCSVToAvro(split.getMiddle(), split.getRight(), csvFileName,
                    avroFileName, ERROR_FILE, csvFilePrinter, context);
            convertCSVToAvro.process();
        } catch (IOException e) {
            LOG.info(String.format("IOException %s happened when process csv file", e.getMessage()));
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
            boolean isISO8601 = TimeStampConvertUtils.SYSTEM_JAVA_TIME_ZONE.equals(timeZone);
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
    protected void cleanup(Context context) {
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
        mergeErrorFile();
    }

    private void mergeErrorFile() {
        String outputPathStr = outputPath.toString();
        String errorFile = outputPathStr + "/" + ImportProperty.ERROR_FILE;
        try (FileSystem fs = HdfsUtils.getFileSystem(conf, errorFile)) {
            Path path = new Path(errorFile);
            // if the error file doesn't exist, need to merge it
            if (!fs.exists(path)) {
                HdfsUtils.HdfsFilenameFilter hdfsFilenameFilter = filename -> filename.startsWith(ERROR_FILE_NAME);
                List<String> errorPaths = HdfsUtils.getFilesForDir(conf, outputPathStr, hdfsFilenameFilter);
                errorPaths.sort(String::compareTo);
                LOG.info("Generated error file list is {}", errorPaths);
                try (FSDataOutputStream fsDataOutputStream = fs.create(path)) {
                    fsDataOutputStream.writeBytes(StringUtils.join(ImportProperty.ERROR_HEADER, ","));
                    fsDataOutputStream.writeBytes(CRLF);
                    for (String errorPath : errorPaths) {
                        try (InputStream inputStream = HdfsUtils.getInputStream(conf, errorPath)) {
                            IOUtils.copy(inputStream, fsDataOutputStream);
                        }
                        fs.delete(new Path(errorPath), false);
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(String.format("IOException happened during the process for merge file: %s.", e.getMessage()));
        }
    }

    private class ConvertCSVToAvro {

        private Map<String, String> errorMap = new HashMap<>();

        private Map<String, String> duplicateMap = new HashMap<>();

        private CSVPrinter csvFilePrinter;

        private String avroFileName;

        private boolean missingRequiredColValue = Boolean.FALSE;

        private boolean fieldMalFormed = Boolean.FALSE;

        private boolean rowError = Boolean.FALSE;

        private GenericRecord avroRecord;

        private String ERROR_FILE;

        private boolean hasAvroRecord = false;

        private boolean hasErrorRecord = false;

        private Context context;

        private String csvFileName;

        private long lineNum = 2;

        private long start;

        private long length;

        ConvertCSVToAvro(long start, long length, String csvFileName, String avroFileName,
                         String ERROR_FILE,
                         CSVPrinter csvFilePrinter, Context context) {
            this.start = start;
            this.length = length;
            this.avroRecord = new GenericData.Record(schema);
            this.csvFileName = csvFileName;
            this.avroFileName = avroFileName;
            this.ERROR_FILE = ERROR_FILE;
            this.csvFilePrinter = csvFilePrinter;
            this.context = context;
        }

        private void process() throws IOException {
            String[] headers;
            DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
            try (DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter)) {
                dataFileWriter.create(schema, new File(avroFileName));
                CSVFormat format = LECSVFormat.format.withFirstRecordAsHeader();
                try (CSVParser parser = new CSVParser(new BufferedReader(new InputStreamReader(
                        new BOMInputStream(new FileInputStream(csvFileName), false, ByteOrderMark.UTF_8,
                                ByteOrderMark.UTF_16LE, ByteOrderMark.UTF_16BE, ByteOrderMark.UTF_32LE,
                                ByteOrderMark.UTF_32BE), StandardCharsets.UTF_8)), format)) {
                    headers = new ArrayList<>(parser.getHeaderMap().keySet()).toArray(new String[]{});
                    Iterator<CSVRecord> iter = parser.iterator();
                    CSVRecord csvRecord = iter.next();
                    if (start > 0) {
                        // need to handle line which is not the real start of the line, sometimes csv one record can
                        // contain multi lines.
                        while (start > csvRecord.getCharacterPosition()) {
                            try {
                                csvRecord = iter.next();
                                lineNum++;
                            } catch (NoSuchElementException e) {
                                LOG.warn(String.format("No records for split file start: %s, length: %s.", start, length));
                                return;
                            } catch (Exception e) {
                                LOG.warn(String.format("Exception %s happened when skipped csv file ", e.getMessage()));
                            }
                        }
                    }
                    long endPosition = start + length - 1l;
                    if (endPosition < csvRecord.getCharacterPosition()) {
                        // the split size is less than one record length or start position is greater than the last
                        // record start position, so just skip this parse thread.
                        LOG.warn(String.format("Skip file split start: %s, length: %s, since split size is less than" +
                                " one SCV record size.", start, length));
                        return;
                    }
                    boolean firstRecord = true;
                    while (true) {
                        // capture IO exception produced during dealing with line
                        try {
                            beforeEachRecord();
                            GenericRecord currentAvroRecord;
                            if (firstRecord) {
                                firstRecord = false;
                            } else {
                                csvRecord = iter.next();
                            }
                            if (endPosition < csvRecord.getCharacterPosition()) {
                                // reach the end of split file
                                LOG.warn(String.format("Reach the end of split file start: %s, length: %s.", start, length));
                                break;
                            }
                            currentAvroRecord = toGenericRecord(Sets.newHashSet(headers), csvRecord);
                            if (errorMap.size() == 0 && duplicateMap.size() == 0) {
                                dataFileWriter.append(currentAvroRecord);
                                importedRecords.increment();
                                hasAvroRecord = true;
                            } else {
                                if (errorMap.size() > 0) {
                                    handleError(lineNum);
                                }
                                if (duplicateMap.size() > 0) {
                                    handleDuplicate(lineNum);
                                }
                            }
                            lineNum++;
                        } catch (IllegalStateException ex) {
                            LOG.warn(ex.getMessage(), ex);
                            rowError = true;
                            errorMap.put(String.valueOf(lineNum),
                                    String.format("CSV parser can't parse this line due to %s", ex.getMessage()));
                            handleError(lineNum);
                        } catch (NoSuchElementException e) {
                            break;
                        }

                    }
                } catch (Exception e) {
                    LOG.warn(e.getMessage(), e);
                }
            }
            csvFilePrinter.close();
            if (hasAvroRecord) {
                HdfsUtils.copyLocalToHdfs(context.getConfiguration(), avroFileName, outputPath + "/" + avroFileName);
            }
            if (hasErrorRecord) {
                HdfsUtils.copyLocalToHdfs(context.getConfiguration(), ERROR_FILE, outputPath + "/" + ERROR_FILE);
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

        private GenericRecord toGenericRecord(Set<String> headers, CSVRecord csvRecord) {
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
        }

        private void beforeEachRecord() {
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
