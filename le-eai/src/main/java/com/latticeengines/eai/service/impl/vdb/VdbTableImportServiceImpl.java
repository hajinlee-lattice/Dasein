package com.latticeengines.eai.service.impl.vdb;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TimeStampConvertUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportVdbProperty;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.domain.exposed.metadata.VdbImportStatus;
import com.latticeengines.domain.exposed.pls.VdbGetQueryData;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.domain.exposed.pls.VdbQueryDataResult;
import com.latticeengines.domain.exposed.pls.VdbQueryResultColumn;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.vdb.converter.VdbTableToAvroTypeConverter;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.format.number.NumberFormatter;
import org.springframework.stereotype.Component;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

@Component("vdbTableImportService")
public class VdbTableImportServiceImpl extends ImportService {

    private static final Log log = LogFactory.getLog(VdbTableImportServiceImpl.class);

    public VdbTableImportServiceImpl() {
        super(SourceType.VISIDB);
    }

    @Autowired
    private VdbTableToAvroTypeConverter vdbTableToAvroTypeConverter;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Value("${eai.vdb.rows.batch:2000}")
    private int rowsToGet;

    @Value("${eai.vdb.cells.batch:1000000}")
    private int cellsPerFile;


    @Override
    public List<Table> importMetadata(SourceImportConfiguration extractionConfig, ImportContext context) {
        String queryHandle = context.getProperty(ImportVdbProperty.VDB_QUERY_HANDLE, String.class);
        String statusUrl = context.getProperty(ImportVdbProperty.REPORT_STATUS_ENDPOINT, String.class);
        VdbLoadTableStatus vdbLoadTableStatus = new VdbLoadTableStatus();
        vdbLoadTableStatus.setVisiDBQueryHandle(queryHandle);
        vdbLoadTableStatus.setJobStatus("Running");
        vdbLoadTableStatus.setMessage("Start import metadata for Vdb table");
        reportStatus(statusUrl, vdbLoadTableStatus);
        List<Table> tables = extractionConfig.getTables();
        List<Table> newTables = new ArrayList<>();
        String metaDataJson = context.getProperty(ImportVdbProperty.METADATA_LIST, String.class);
        log.info(String.format("Meta data json string: %s", metaDataJson));
        @SuppressWarnings("unchecked")
        List<Object> rawList = JsonUtils.deserialize(metaDataJson, List.class);
        List<VdbSpecMetadata> vdbSpecMetadataList = JsonUtils.convertList(rawList, VdbSpecMetadata.class);
        HashMap<String, VdbSpecMetadata> vdbSpecMetadataHashMap = new HashMap<>();
        for (VdbSpecMetadata metadata : vdbSpecMetadataList) {
            vdbSpecMetadataHashMap.put(metadata.getColumnName(), metadata);
        }
        try {
            for(Table table : tables) {
                List<Attribute> attributes = table.getAttributes();
                for (Attribute attribute : attributes) {
                    if (attribute == null) {
                        log.warn(String.format("Empty attribute in table %s", table.getName()));
                        continue;
                    }
                    if (vdbSpecMetadataHashMap.containsKey(attribute.getName())) {
                        log.info(String.format("Processing attribute %s with logical type %s.", attribute.getName(),
                                vdbSpecMetadataHashMap.get(attribute.getName()).getDataType()));

                        if (vdbSpecMetadataHashMap.get(attribute.getName()) != null) {
                            attribute.setSourceLogicalDataType(vdbSpecMetadataHashMap.get(attribute.getName()).getDataType());
                            attribute.setPhysicalDataType(vdbTableToAvroTypeConverter.convertTypeToAvro(
                                    attribute.getSourceLogicalDataType().toLowerCase()).name());

                            if (attribute.getSourceLogicalDataType().toLowerCase().equals("date")) {
                                attribute.setPropertyValue("dateFormat", "YYYY-MM-DD");
                            }
                        }
                    }
                }
                Schema schema = TableUtils.createSchema(table.getName(), table);
                table.setSchema(schema);
                newTables.add(table);
                setExtractContextForVdbTable(table, context);
            }
        } catch (Exception e) {
            log.error(String.format("Import table metadata failed with exception: %s", e.toString()));
            vdbLoadTableStatus.setJobStatus("Failed");
            vdbLoadTableStatus.setMessage(String.format("Import table metadata failed with exception: %s", e.toString()));
            reportStatus(statusUrl, vdbLoadTableStatus);
            throw new RuntimeException(vdbLoadTableStatus.getMessage());
        }
        return newTables;
    }

    @SuppressWarnings("unchecked")
    public void setExtractContextForVdbTable(Table table, ImportContext context) {
        Configuration config = context.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String defaultFS = config.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String targetPath = context.getProperty(ImportVdbProperty.TARGETPATH, String.class);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        context.getProperty(ImportVdbProperty.EXTRACT_PATH, Map.class).put(table.getName(), hdfsUri);
        context.getProperty(ImportVdbProperty.PROCESSED_RECORDS, Map.class).put(table.getName(), 0L);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration extractionConfig, ImportContext context) {

        List<Table> tables = extractionConfig.getTables();
        Table table = tables.get(0);

        String queryDataUrl = context.getProperty(ImportVdbProperty.QUERY_DATA_ENDPOINT, String.class);
        String statusUrl = context.getProperty(ImportVdbProperty.REPORT_STATUS_ENDPOINT, String.class);
        String queryHandle = context.getProperty(ImportVdbProperty.VDB_QUERY_HANDLE, String.class);
        VdbLoadTableStatus vdbLoadTableStatus = new VdbLoadTableStatus();
        vdbLoadTableStatus.setVisiDBQueryHandle(queryHandle);
        String avroFileName = context.getProperty(ImportVdbProperty.EXTRACT_NAME, String.class);
        String targetPath = context.getProperty(ImportVdbProperty.TARGETPATH, String.class);
        String extractIdentifier = context.getProperty(ImportVdbProperty.EXTRACT_IDENTIFIER, String.class);
        String customSpace = context.getProperty(ImportVdbProperty.CUSTOMER, String.class);
        Configuration yarnConfiguration = context.getProperty(ImportVdbProperty.HADOOPCONFIG, Configuration.class);
        String metaDataJson = context.getProperty(ImportVdbProperty.METADATA_LIST, String.class);
        @SuppressWarnings("unchecked")
        List<Object> rawList = JsonUtils.deserialize(metaDataJson, List.class);
        List<VdbSpecMetadata> vdbSpecMetadataList = JsonUtils.convertList(rawList, VdbSpecMetadata.class);

        VdbImportExtract vdbImportExtract = eaiMetadataService.getVdbImportExtract(customSpace, extractIdentifier);
        if (vdbImportExtract == null) {
            log.error(String.format("Cannot find Vdb import extract record for identifier %s", extractIdentifier));
            vdbLoadTableStatus.setJobStatus("Failed");
            vdbLoadTableStatus.setMessage(String.format("Cannot find Vdb import extract record for identifier %s",
                    extractIdentifier));
            reportStatus(statusUrl, vdbLoadTableStatus);
            throw new RuntimeException(vdbLoadTableStatus.getMessage());
        }
        int proccessedLines = vdbImportExtract.getProcessedRecords();
        int linesPerFile = vdbImportExtract.getLinesPerFile();
        int fileIndex = 0;
        if (proccessedLines > 0) {
            fileIndex = proccessedLines / linesPerFile;
        }
        if (linesPerFile == 0) {
            linesPerFile = getLinesPerFile(vdbSpecMetadataList.size());
            vdbImportExtract.setLinesPerFile(linesPerFile);
            eaiMetadataService.updateVdbImportExtract(customSpace, vdbImportExtract);
        }

        int totalRows;
        try {
            totalRows = Integer.parseInt(context.getProperty(ImportVdbProperty.TOTAL_ROWS, String.class));
        } catch (Exception e) {
            throw new RuntimeException(String.format("Can't parse total rows value: %s",
                    context.getProperty(ImportVdbProperty.TOTAL_ROWS, String.class)));
        }
        int startRow = proccessedLines;
        int fileRows = 0;

        Schema schema = table.getSchema();
        log.info("schema is: " + schema.toString());

        VdbGetQueryData vdbGetQueryData = new VdbGetQueryData();
        vdbGetQueryData.setVdbQueryHandle(queryHandle);
        vdbGetQueryData.setStartRow(startRow);
        vdbGetQueryData.setRowsToGet(rowsToGet);


        vdbLoadTableStatus.setJobStatus("Running");
        vdbLoadTableStatus.setMessage("Start import data for Vdb table");
        reportStatus(statusUrl, vdbLoadTableStatus);
        boolean error = false;
        boolean needNewFile = true;
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        try {
            HashMap<String, Attribute> attributeMap = new HashMap<>();
            for (Attribute attr : table.getAttributes()) {
                attributeMap.put(attr.getName(), attr);
            }
            DataFileWriter<GenericRecord> dataFileWriter = null;
            while(!error) {
                try {
                    if (needNewFile) {
                        dataFileWriter = newDataFileWriter(schema, outputStream);
                        needNewFile = false;
                    }
                    VdbQueryDataResult vdbQueryDataResult = dataLoaderService.getQueryDataResult(queryDataUrl,
                            vdbGetQueryData);
                    if (vdbQueryDataResult == null) {
                        break;
                    }
                    int rowsAppend = appendGenericRecord(attributeMap, schema, vdbQueryDataResult, dataFileWriter);
                    log.info(String.format("Append %d rows to avro file", rowsAppend));

                    startRow += rowsAppend;
                    fileRows += rowsAppend;

                    vdbGetQueryData.setStartRow(startRow);
                    vdbLoadTableStatus.setJobStatus("Running");
                    vdbLoadTableStatus.setMessage(
                            String.format("%d of total %d records are processed.", startRow, totalRows));

                    reportStatus(statusUrl, vdbLoadTableStatus);

                    if (fileRows == linesPerFile) {
                        closeDataFileWriter(dataFileWriter);
                        String fileName = generateAvroFileName(avroFileName, fileIndex);
                        writeStreamToHdfs(outputStream, targetPath + "/" + fileName, yarnConfiguration);
                        fileIndex++;
                        fileRows = 0;
                        needNewFile = true;
                        vdbImportExtract.setProcessedRecords(startRow);
                        eaiMetadataService.updateVdbImportExtract(customSpace, vdbImportExtract);
                    }

                } catch (Exception e) {
                    error = true;
                    log.error(String.format("Load table failed with exception: %s", e));
                    vdbLoadTableStatus.setJobStatus("Failed");
                    vdbLoadTableStatus.setMessage(
                            String.format("Load table failed with exception: %s", e.toString()));
                }
                if (startRow >= totalRows) {
                    closeDataFileWriter(dataFileWriter);
                    break;
                }
            }
        } catch (Exception e) {
            log.error(String.format("Load table failed with exception: %s", e));
            error = true;
            vdbLoadTableStatus.setJobStatus("Failed");
            vdbLoadTableStatus.setMessage(String.format("Load table failed with exception: %s", e.toString()));
        }
        if (!error) {
            vdbLoadTableStatus.setJobStatus("Succeed");
            vdbLoadTableStatus.setMessage("Load table complete!");
            reportStatus(statusUrl, vdbLoadTableStatus);
            context.getProperty(ImportVdbProperty.PROCESSED_RECORDS, Map.class).put(table.getName(),
                    new Long(totalRows));
            String fileName = generateAvroFileName(avroFileName, fileIndex);
            writeStreamToHdfs(outputStream, targetPath + "/" + fileName, yarnConfiguration);
            fileIndex++;
            vdbImportExtract.setProcessedRecords(totalRows);
            vdbImportExtract.setStatus(VdbImportStatus.SUCCESS);
            eaiMetadataService.updateVdbImportExtract(customSpace, vdbImportExtract);
        } else {
            throw new RuntimeException(vdbLoadTableStatus.getMessage());
        }
    }

    private void reportStatus(String url, VdbLoadTableStatus status) {
        try {
            dataLoaderService.reportGetDataStatus(url, status);
        } catch (Exception e) {
            log.warn("Error reporting status");
        }
    }

    private int appendGenericRecord(HashMap<String, Attribute> attributeMap, Schema schema,
                                    VdbQueryDataResult vdbQueryDataResult,
                                    DataFileWriter<GenericRecord> dataFileWriter) throws IOException {
        int rowSize = vdbQueryDataResult.getColumns().get(0).getValues().size();
        int rowsAppend = 0;
        for (int i = 0; i < rowSize; i++) {
            GenericRecord avroRecord = new GenericData.Record(schema);
            for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
                if (attributeMap.containsKey(column.getColumnName())) {
                    Attribute attr = attributeMap.get(column.getColumnName());
                    Type avroType = schema.getField(
                            attributeMap.get(column.getColumnName()).getName()).schema().getTypes().get(0).getType();
                    try {
                        String vdbFieldValue = column.getValues().get(i);
                        if (!StringUtils.isEmpty(vdbFieldValue)) {
                            if (vdbFieldValue.toLowerCase().equals("null")) {
                                log.warn(String.format("Value for %s column is null", attr.getName()));
                            } else {
                                Object avroFieldValue = toAvro(vdbFieldValue, avroType, attr);
                                avroRecord.put(column.getColumnName(), avroFieldValue);
                            }
                        }
                    } catch (Exception e) {
                        log.error(e.getMessage());
                    }
                }
            }
            dataFileWriter.append(avroRecord);
            rowsAppend++;
        }
        return rowsAppend;
    }

    private Object toAvro(String fieldValue, Type avroType, Attribute attr) {
        try {
            switch (avroType) {
                case DOUBLE:
                    return new Double(parseStringToNumber(fieldValue).doubleValue());
                case FLOAT:
                    return new Float(parseStringToNumber(fieldValue).floatValue());
                case INT:
                    return new Integer(parseStringToNumber(fieldValue).intValue());
                case LONG:
                    if (attr.getSourceLogicalDataType() != null &&
                            (attr.getSourceLogicalDataType().toLowerCase().equals("date") ||
                            attr.getSourceLogicalDataType().toLowerCase().equals("datetime") ||
                            attr.getSourceLogicalDataType().toLowerCase().equals("datetimeoffset"))) {
                        log.info("Date value from vdb: " + fieldValue);
                        return new Long(TimeStampConvertUtils.convertToLong(fieldValue));
                    } else {
                        return new Long(parseStringToNumber(fieldValue).longValue());
                    }
                case STRING:
                    return fieldValue;
                case ENUM:
                    return fieldValue;
                case BOOLEAN:
                    if (fieldValue.equals("1") || fieldValue.equalsIgnoreCase("true")) {
                        return Boolean.TRUE;
                    } else if (fieldValue.equals("0") || fieldValue.equalsIgnoreCase("false")) {
                        return Boolean.FALSE;
                    }
                default:
                    log.info("size is:" + fieldValue.length());
                    throw new IllegalArgumentException(
                            "Not supported Field, avroType: " + avroType + ", physicalDatalType:");
            }
        } catch (IllegalArgumentException e) {
            log.warn(e.getMessage());
            throw new RuntimeException(
                    String.format("Cannot convert %s to type %s for column %s.", fieldValue, avroType, ""));
        } catch (Exception e) {
            log.warn(e.getMessage());
            throw new RuntimeException(String.format("Cannot parse %s as %s for column %s.", fieldValue, avroType,
                    attr.getName()));
        }
    }

    private void writeStreamToHdfs(ByteArrayOutputStream outputStream, String filePath,
            Configuration yarnConfiguration) {
        ByteArrayInputStream inputStream = new ByteArrayInputStream(outputStream.toByteArray());
        try {
            HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, inputStream, filePath);
        } catch (IOException e) {
            log.error(String.format("Cannot write stream to Hdfs. Exception: %s", e.toString()));
        }
        outputStream.reset();
    }

    @VisibleForTesting
    private Number parseStringToNumber(String inputStr) throws ParseException {
        NumberFormatter numberFormatter = new NumberFormatter();
        return numberFormatter.parse(inputStr, Locale.getDefault());
    }

    private int getLinesPerFile(int columnCount) {
        int linesPerFile = cellsPerFile / columnCount;
        int batch = linesPerFile / rowsToGet;
        if (batch == 0) {
            batch = 1;
        }
        linesPerFile = batch * rowsToGet;
        return linesPerFile;
    }

    private String generateAvroFileName(String fileName, int fileIndex) {
        int extensionPos = fileName.lastIndexOf('.');
        String name = fileName.substring(0, extensionPos);
        String extension = fileName.substring(extensionPos);
        return String.format("%s_%d%s", name, fileIndex, extension);
    }

    private DataFileWriter<GenericRecord> newDataFileWriter(Schema schema, ByteArrayOutputStream outputStream) {
        DatumWriter<GenericRecord> userDatumWriter = new GenericDatumWriter<>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(userDatumWriter);
        try {
            dataFileWriter.create(schema, outputStream);
        } catch (IOException e) {
            log.error("Error creating data file writer!");
        }
        return dataFileWriter;
    }

    private void closeDataFileWriter(DataFileWriter<GenericRecord> dataFileWriter) {
        try {
            dataFileWriter.close();
        } catch (IOException e) {
            log.error("Error closing data file writer");
        }
    }
}
