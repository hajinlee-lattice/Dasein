package com.latticeengines.eai.service.impl.vdb;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dataloader.DataReadyResult;
import com.latticeengines.domain.exposed.dataloader.GetDataTablesResult;
import com.latticeengines.domain.exposed.dataloader.LaunchIdQuery;
import com.latticeengines.domain.exposed.eai.ConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.ImportVdbProperty;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbGetQueryData;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.domain.exposed.pls.VdbQueryDataResult;
import com.latticeengines.domain.exposed.pls.VdbQueryResultColumn;
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.eai.routes.DataContainer;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.vdb.converter.VdbTableToAvroTypeConverter;
import com.latticeengines.eai.service.impl.vdb.converter.VdbValueConverter;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("vdbTableImportService")
public class VdbTableImportServiceImpl extends ImportService {

    private static final Logger log = LoggerFactory.getLogger(VdbTableImportServiceImpl.class);

    public VdbTableImportServiceImpl() {
        super(SourceType.VISIDB);
    }

    public static final String EXTRACT_DATE_FORMAT = "yyyy-MM-dd-HH-mm-ss";

    @Autowired
    private VdbTableToAvroTypeConverter vdbTableToAvroTypeConverter;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Value("${eai.vdb.file.size:10000000}")
    private int sizePerFile;

    @Value("${eai.vdb.extract.size:1000000}")
    private int recordsPerExtract;

    private static final int MAX_RETRIES = 3;

    @Override
    public ConnectorConfiguration generateConnectorConfiguration(String connectorConfig, ImportContext context) {
        VdbConnectorConfiguration vdbConnectorConfiguration = JsonUtils.deserialize(connectorConfig,
                VdbConnectorConfiguration.class);
        log.info(String.format("DL data ready: %b", vdbConnectorConfiguration.isDlDataReady()));
        if (!vdbConnectorConfiguration.isDlDataReady()) {
            long launchId;
            try {
                launchId = dataLoaderService.executeLoadGroup(vdbConnectorConfiguration.getDlTenantId(),
                        vdbConnectorConfiguration.getDlLoadGroup(), vdbConnectorConfiguration.getDlEndpoint());
            } catch (Exception e) {
                throw new LedpException(LedpCode.LEDP_17011, new String[] { e.toString() });
            }
            LaunchIdQuery query = new LaunchIdQuery();
            query.setLaunchId(launchId);
            DataReadyResult readyResult = null;
            int exceptionCount = 0;
            do {
                try {
                    readyResult = dataLoaderService.readyToExportData(vdbConnectorConfiguration.getDlEndpoint(), query);
                } catch (Exception e) {
                    exceptionCount++;
                    try {
                        Thread.sleep(30000L);
                    } catch (InterruptedException e1) {
                        // do nothing.
                    }
                }
                try {
                    Thread.sleep(10000L);
                } catch (InterruptedException e) {
                    // do nothing.
                }
            } while (exceptionCount < MAX_RETRIES && (readyResult == null || !readyResult.isDataReady()));
            if (!readyResult.isDataReady()) {
                cancelLoadgroup(vdbConnectorConfiguration.getDlEndpoint(), (int) launchId,
                        "Cannot get loadgroup status from DL!");
                throw new LedpException(LedpCode.LEDP_17012);
            }
            GetDataTablesResult tablesResult = null;
            try {
                tablesResult = dataLoaderService.getDataTables(vdbConnectorConfiguration.getDlEndpoint(), query);
            } catch (Exception e) {
                cancelLoadgroup(vdbConnectorConfiguration.getDlEndpoint(), (int) launchId, e.toString());
                throw new LedpException(LedpCode.LEDP_17013, new String[] { e.toString() });
            }
            if (tablesResult.isSuccess()) {
                String dateTime = new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date());
                for (VdbLoadTableConfig tableConfig : tablesResult.getTableConfigs()) {
                    ImportVdbTableConfiguration importVdbTableConfiguration = new ImportVdbTableConfiguration();
                    CustomerSpace customerSpace = CustomerSpace.parse(tableConfig.getTenantId());
                    String collectionIdentifier = String.format("%s_%s_%s", customerSpace.toString(),
                            tableConfig.getTableName(), tableConfig.getLaunchId());
                    importVdbTableConfiguration.setBatchSize(tableConfig.getBatchSize());
                    importVdbTableConfiguration.setDataCategory(tableConfig.getDataCategory());
                    importVdbTableConfiguration.setCollectionIdentifier(collectionIdentifier);
                    importVdbTableConfiguration.setVdbQueryHandle(tableConfig.getVdbQueryHandle());
                    importVdbTableConfiguration.setMergeRule(tableConfig.getMergeRule());
                    importVdbTableConfiguration.setMetadataList(tableConfig.getMetadataList());
                    importVdbTableConfiguration.setTotalRows(tableConfig.getTotalRows());
                    importVdbTableConfiguration.setCreateTableRule(tableConfig.getCreateTableRule());
                    String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                            PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                            SourceType.VISIDB.getName(), dateTime);
                    importVdbTableConfiguration.setExtractPath(targetPath);
                    if (StringUtils.isEmpty(vdbConnectorConfiguration.getGetQueryDataEndpoint())) {
                        vdbConnectorConfiguration.setGetQueryDataEndpoint(tableConfig.getGetQueryDataEndpoint());
                    }
                    if (StringUtils.isEmpty(vdbConnectorConfiguration.getReportStatusEndpoint())) {
                        vdbConnectorConfiguration.setReportStatusEndpoint(tableConfig.getReportStatusEndpoint());
                    }
                    vdbConnectorConfiguration.addTableConfiguration(tableConfig.getTableName(),
                            importVdbTableConfiguration);
                }
            } else {
                throw new LedpException(LedpCode.LEDP_17013, new String[] { tablesResult.getErrorMessage() });
            }

        } else {
            CustomerSpace customerSpace = CustomerSpace
                    .parse(context.getProperty(ImportProperty.CUSTOMER, String.class));
            for (Map.Entry<String, ImportVdbTableConfiguration> entry : vdbConnectorConfiguration
                    .getTableConfigurations().entrySet()) {
                entry.getValue().setExtractPath(
                        getExtractTargetPath(customerSpace, entry.getValue().getCollectionIdentifier()));
            }
        }
        return vdbConnectorConfiguration;
    }

    private String getExtractTargetPath(CustomerSpace customerSpace, String collectionIdentifier) {
        EaiImportJobDetail eaiImportJobDetail = eaiImportJobDetailService
                .getImportJobDetailByCollectionIdentifier(collectionIdentifier);
        String targetPath = "";
        if (eaiImportJobDetail != null && eaiImportJobDetail.getStatus() == ImportStatus.FAILED) {
            targetPath = eaiImportJobDetail.getTargetPath();
        }
        if (StringUtils.isEmpty(targetPath)) {
            targetPath = generateNewTargetPath(customerSpace, null);
        }
        return targetPath;
    }

    private String generateNewTargetPath(CustomerSpace customerSpace, Configuration yarnConfiguration) {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        if (yarnConfiguration != null) {
            try {
                int i = 1;
                while(HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                    targetPath = String.format("%s/%s/DataFeed1/DataFeed1-Account/Extracts/%s",
                            PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                            SourceType.VISIDB.getName(),
                            new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date(System.currentTimeMillis() +
                                    i * 1000L)));
                    i++;
                }
            } catch (IOException e) {
                log.error("Cannot access hdfs!");
            }
        }
        return targetPath;
    }

    private void cancelLoadgroup(String dlEndpoint, int launchId, String message) {
        try {
            dataLoaderService.editLaunchStatus(dlEndpoint, launchId, "Failed", message);
        } catch (Exception e) {
            // do noting.
        }
    }

    @Override
    public List<Table> importMetadata(SourceImportConfiguration extractionConfig, ImportContext context,
            ConnectorConfiguration connectorConfiguration) {
        return null;
    }

    @Override
    public List<Table> prepareMetadata(List<Table> originalTables) {
        List<Table> metaData = new ArrayList<>();
        for (Table table : originalTables) {
            for (Attribute attribute : table.getAttributes()) {
                if (attribute == null) {
                    log.warn(String.format("Empty attribute in table %s", table.getName()));
                    continue;
                }

                if (StringUtils.isBlank(attribute.getSourceLogicalDataType())) {
                    attribute.setSourceLogicalDataType(attribute.getPhysicalDataType());
                }
                if (StringUtils.isBlank(attribute.getSourceLogicalDataType())) {
                    throw new RuntimeException("Attribute " + attribute.getName() //
                            + " has neither physical data type nor source logical data type");
                }
                attribute.setPhysicalDataType(vdbTableToAvroTypeConverter
                        .convertTypeToAvro(attribute.getSourceLogicalDataType().toLowerCase()).name());
                if (attribute.getSourceLogicalDataType().toLowerCase().equals("date")) {
                    attribute.setPropertyValue("dateFormat", "YYYY-MM-DD");
                }
            }
            Schema schema = TableUtils.createSchema(table.getName(), table);
            table.setSchema(schema);
            metaData.add(table);
        }
        return metaData;
    }

    private Table createTable(String customerSpace, String tableName, ImportVdbTableConfiguration importTableConfig) {
        Table table;
        table = eaiMetadataService.getTable(customerSpace.toString(), tableName);
        VdbCreateTableRule createRule = VdbCreateTableRule.getCreateRule(importTableConfig.getCreateTableRule());
        if (table == null) {
            if (createRule == VdbCreateTableRule.UPDATE) {
                throw new LedpException(LedpCode.LEDP_18142, new String[] { tableName });
            }
            table = new Table();
        } else {
            if (createRule == VdbCreateTableRule.CREATE_NEW) {
                throw new LedpException(LedpCode.LEDP_18140, new String[] { tableName });
            }
        }
        if (verifyAndUpdateTableAttr(table, importTableConfig.getMetadataList())) {
            table.setPrimaryKey(null);
            table.setName(tableName);
            table.setDisplayName(tableName);
        } else {
            throw new LedpException(LedpCode.LEDP_18141);
        }
        return table;
    }

    private boolean verifyAndUpdateTableAttr(Table table, List<VdbSpecMetadata> metadataList) {
        boolean result = true;
        HashMap<String, Attribute> attrMap = new HashMap<>();
        for (Attribute attr : table.getAttributes()) {
            attrMap.put(attr.getName(), attr);
        }
        for (VdbSpecMetadata metadata : metadataList) {
            if (!attrMap.containsKey(metadata.getColumnName())) {
                Attribute attr = new Attribute();
                attr.setName(metadata.getColumnName());
                attr.setDisplayName(metadata.getDisplayName());
                attr.setLogicalDataType(metadata.getDataType());
                table.addAttribute(attr);
            } else {
                if (!attrMap.get(metadata.getColumnName()).getSourceLogicalDataType().toLowerCase()
                        .equals(metadata.getDataType().toLowerCase())) {
                    result = false;
                    break;
                }
            }
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    private void setExtractContextForVdbTable(Table table, ImportContext context,
            ImportVdbTableConfiguration tableConfig, boolean multipleExtract) {
        Configuration config = context.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String defaultFS = config.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String targetPath = tableConfig.getExtractPath();
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        context.getProperty(ImportVdbProperty.EXTRACT_PATH, Map.class).put(table.getName(), hdfsUri);
        context.getProperty(ImportVdbProperty.PROCESSED_RECORDS, Map.class).put(table.getName(), 0L);
        if (multipleExtract) {
            context.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class).put(table.getName(), Boolean.TRUE);
            context.getProperty(ImportProperty.EXTRACT_PATH_LIST, Map.class).put(table.getName(),
                    new ArrayList<String>());
            context.getProperty(ImportProperty.EXTRACT_RECORDS_LIST, Map.class).put(table.getName(),
                    new ArrayList<Long>());
        } else {
            context.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class).put(table.getName(), Boolean.FALSE);
        }
    }

    @SuppressWarnings("unchecked")
    public void updateExtractContext(Table table, ImportContext context, String targetPath, Long processedRecords) {
        Configuration config = context.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String defaultFS = config.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        List.class.cast(context.getProperty(ImportVdbProperty.EXTRACT_PATH_LIST, Map.class).get(table.getName()))
                .add(hdfsUri);
        List.class.cast(context.getProperty(ImportVdbProperty.EXTRACT_RECORDS_LIST, Map.class).get(table.getName()))
                .add(processedRecords);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void importDataAndWriteToHdfs(SourceImportConfiguration extractionConfig, ImportContext context,
            ConnectorConfiguration connectorConfiguration) {
        VdbConnectorConfiguration vdbConnectorConfiguration = null;
        if (connectorConfiguration instanceof VdbConnectorConfiguration) {
            vdbConnectorConfiguration = (VdbConnectorConfiguration) connectorConfiguration;
        } else {
            throw new LedpException(LedpCode.LEDP_17010);
        }
        CustomerSpace customerSpace = CustomerSpace.parse(context.getProperty(ImportProperty.CUSTOMER, String.class));
        List<Table> tables = extractionConfig.getTables();
        for (Table table : tables) {
            ImportVdbTableConfiguration importVdbTableConfiguration = vdbConnectorConfiguration
                    .getVdbTableConfiguration(table.getName());
            if (importVdbTableConfiguration == null) {
                continue;
            }
            long startTime = System.currentTimeMillis();
            int extractRecords = 0;
            try {
                String queryDataUrl = vdbConnectorConfiguration.getGetQueryDataEndpoint();
                String statusUrl = vdbConnectorConfiguration.getReportStatusEndpoint();
                String queryHandle = importVdbTableConfiguration.getVdbQueryHandle();
                VdbLoadTableStatus vdbLoadTableStatus = new VdbLoadTableStatus();
                vdbLoadTableStatus.setVdbQueryHandle(queryHandle);
                String avroFileName = String.format("Extract_%s.avro", table.getName());
                String targetPath = importVdbTableConfiguration.getExtractPath();
                String extractIdentifier = importVdbTableConfiguration.getCollectionIdentifier();
                Configuration yarnConfiguration = context.getProperty(ImportVdbProperty.HADOOPCONFIG,
                        Configuration.class);
                int totalRows = importVdbTableConfiguration.getTotalRows();
                int rowsToGet = importVdbTableConfiguration.getBatchSize();
                boolean needMultipleExtracts = totalRows > recordsPerExtract;
                setExtractContextForVdbTable(table, context, importVdbTableConfiguration, needMultipleExtracts);
                EaiImportJobDetail importJobDetail = eaiImportJobDetailService
                        .getImportJobDetailByCollectionIdentifier(extractIdentifier);
                if (importJobDetail == null) {
                    log.error(String.format("Cannot find Vdb import extract record for identifier %s",
                            extractIdentifier));
                    vdbLoadTableStatus.setJobStatus("Failed");
                    vdbLoadTableStatus.setMessage(String
                            .format("Cannot find Vdb import extract record for identifier %s", extractIdentifier));
                    reportStatus(statusUrl, vdbLoadTableStatus);
                    throw new RuntimeException(vdbLoadTableStatus.getMessage());
                }
                importJobDetail.setStatus(ImportStatus.RUNNING);
                importJobDetail.setReportURL(statusUrl);
                importJobDetail.setQueryHandle(queryHandle);
                eaiImportJobDetailService.updateImportJobDetail(importJobDetail);
                int processedLines = importJobDetail.getProcessedRecords();
                int fileIndex = 0;
                if (processedLines > 0) {
                    fileIndex = getFileIndex(avroFileName, targetPath, yarnConfiguration);
                }
                int startRow = processedLines;
                VdbValueConverter vdbValueConverter = new VdbValueConverter();

                DataContainer dataContainer = new DataContainer(vdbValueConverter, table);

                VdbGetQueryData vdbGetQueryData = new VdbGetQueryData();
                vdbGetQueryData.setVdbQueryHandle(queryHandle);
                vdbGetQueryData.setStartRow(startRow);
                vdbGetQueryData.setRowsToGet(rowsToGet);

                vdbLoadTableStatus.setJobStatus("Running");
                vdbLoadTableStatus.setMessage("Start import data for Vdb table");
                reportStatus(statusUrl, vdbLoadTableStatus);
                boolean error = false;
                boolean needNewFile = true;

                try {
                    HashMap<String, Attribute> attributeMap = new HashMap<>();
                    for (Attribute attr : table.getAttributes()) {
                        attributeMap.put(attr.getSourceAttrName(), attr);
                    }
                    long getDLDataResultTime = 0;
                    while (!error) {
                        try {
                            if (needNewFile) {
                                getDLDataResultTime = 0;
                                dataContainer = new DataContainer(vdbValueConverter, table);
                                needNewFile = false;
                            }
                            VdbQueryDataResult vdbQueryDataResult = null;
                            try {
                                long readDLStartTime = System.currentTimeMillis();
                                vdbQueryDataResult = dataLoaderService.getQueryDataResult(queryDataUrl,
                                        vdbGetQueryData);
                                getDLDataResultTime += System.currentTimeMillis() - readDLStartTime;
                            } catch (Exception e) {
                                log.error(String.format("Errors occur when get data from DL! Exception: %s",
                                        e.toString()));
                                throw new LedpException(LedpCode.LEDP_17016);
                            }
                            if (vdbQueryDataResult == null) {
                                break;
                            }
                            int rowsAppend = appendGenericRecord(attributeMap, dataContainer, vdbQueryDataResult);
                            if (rowsAppend != rowsToGet) {
                                log.warn(String.format("Row batch is %d, but only %d rows append to avro.", rowsToGet,
                                        rowsAppend));
                            }
                            startRow += rowsToGet;
                            if (startRow > totalRows) {
                                startRow = totalRows;
                            }
                            extractRecords += rowsAppend;

                            vdbGetQueryData.setStartRow(startRow);
                            vdbLoadTableStatus.setJobStatus("Running");
                            vdbLoadTableStatus.setMessage(
                                    String.format("%d of total %d records are processed.", startRow, totalRows));
                            vdbLoadTableStatus.setProcessedRecords(startRow);

                            reportStatus(statusUrl, vdbLoadTableStatus);
                            dataContainer.flush();
                            if (dataContainer.getLocalDataFile().length() >= sizePerFile
                                    || (extractRecords + rowsToGet) > recordsPerExtract) {
                                dataContainer.endContainer();
                                String fileName = generateAvroFileName(avroFileName, fileIndex);
                                try (PerformanceTimer timer = new PerformanceTimer("DL copy to hdfs")) {
                                    copyToHdfs(dataContainer, targetPath + "/" + fileName, yarnConfiguration);
                                }
                                fileIndex++;
                                needNewFile = true;
                                importJobDetail.setProcessedRecords(startRow);
                                eaiImportJobDetailService.updateImportJobDetail(importJobDetail);
                                dataContainer.getLocalDataFile().delete();
                                log.info(String.format("Get data from DL takes %d ms", getDLDataResultTime));
                                if ((extractRecords + rowsToGet) > recordsPerExtract) {
                                    if (needMultipleExtracts) {
                                        updateExtractContext(table, context, targetPath, new Long(extractRecords));
                                        fileIndex = 0;
                                        targetPath = generateNewTargetPath(customerSpace, yarnConfiguration);
                                        extractRecords = 0;
                                    }
                                }
                            }

                        } catch (Exception e) {
                            error = true;
                            log.error(String.format("Load table failed with exception: %s", e));
                            vdbLoadTableStatus.setJobStatus("Failed");
                            vdbLoadTableStatus
                                    .setMessage(String.format("Load table failed with exception: %s", e.toString()));
                        }
                        if (startRow >= totalRows) {
                            dataContainer.endContainer();
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
                    context.getProperty(ImportVdbProperty.PROCESSED_RECORDS, Map.class).put(table.getName(),
                            new Long(totalRows));
                    String fileName = generateAvroFileName(avroFileName, fileIndex);
                    boolean fileCopy = copyToHdfs(dataContainer, targetPath + "/" + fileName, yarnConfiguration);
                    fileIndex++;
                    if (needMultipleExtracts && fileCopy) {
                        updateExtractContext(table, context, targetPath, new Long(extractRecords));
                    }
                    importJobDetail.setProcessedRecords(totalRows);
                    importJobDetail.setStatus(ImportStatus.SUCCESS);
                    eaiImportJobDetailService.updateImportJobDetail(importJobDetail);
                    log.info(String.format("Total load data from DL takes %d ms",
                            System.currentTimeMillis() - startTime));
                } else {
                    reportStatus(statusUrl, vdbLoadTableStatus);
                    throw new LedpException(LedpCode.LEDP_17015, new String[] { vdbLoadTableStatus.getMessage() });
                }
            } catch (RuntimeException e) {
                EaiImportJobDetail eaiImportJobDetail = eaiImportJobDetailService
                        .getImportJobDetailByCollectionIdentifier(
                                importVdbTableConfiguration.getCollectionIdentifier());
                eaiImportJobDetail.setStatus(ImportStatus.FAILED);
                eaiImportJobDetailService.updateImportJobDetail(eaiImportJobDetail);
                throw e;
            }
        }
    }

    private void reportStatus(String url, VdbLoadTableStatus status) {
        try {
            dataLoaderService.reportGetDataStatus(url, status);
        } catch (Exception e) {
            log.warn("Error reporting status");
        }
    }

    private int appendGenericRecord(HashMap<String, Attribute> attributeMap, DataContainer dataContainer,
            VdbQueryDataResult vdbQueryDataResult) {
        int rowSize = vdbQueryDataResult.getColumns().get(0).getValues().size();
        int rowsAppend = 0;
        for (int i = 0; i < rowSize; i++) {
            if (validateRecord(attributeMap, vdbQueryDataResult, i)) {
                dataContainer.newRecord();
                for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
                    if (attributeMap.containsKey(column.getColumnName())) {
                        Attribute attr = attributeMap.get(column.getColumnName());
                        dataContainer.setValueForAttribute(attr, column.getValues().get(i));
                    }
                }
                dataContainer.endRecord();
                rowsAppend++;
            }
        }
        return rowsAppend;
    }

    private boolean validateRecord(HashMap<String, Attribute> attributeMap, VdbQueryDataResult vdbQueryDataResult,
                                   int index) {
        boolean result = true;
        for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
            if (attributeMap.containsKey(column.getColumnName())) {
                if (!attributeMap.get(column.getColumnName()).isNullable()) {
                    if (StringUtils.isEmpty(column.getValues().get(index))) {
                        if (attributeMap.get(column.getColumnName()).getDefaultValueStr() != null) {
                            column.getValues()
                                    .set(index, attributeMap.get(column.getColumnName()).getDefaultValueStr());
                        } else {
                            result = false;
                            String record = getVdbRecord(vdbQueryDataResult, index);
                            log.error(String.format("Missing required field: %s. Record values: %s",
                                    attributeMap.get(column.getColumnName()).getName(), record));
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }

    private String getVdbRecord(VdbQueryDataResult vdbQueryDataResult, int index) {
        String result = "";
        for (VdbQueryResultColumn column : vdbQueryDataResult.getColumns()) {
            result += column.getValues().get(index);
            result += ",";
        }
        if (!StringUtils.isEmpty(result)) {
            result = result.substring(0, result.length() - 1);
        }
        return result;
    }


    private boolean copyToHdfs(DataContainer dataContainer, String filePath, Configuration yarnConfiguration) {
        int retries = 0;
        Exception exception = null;
        boolean fileCopy = true;
        log.info(String.format("Copy data container file to HDFS path %s", filePath));
        do {
            try {
                retries++;
                exception = null;
                InputStream dataInputStream = new FileInputStream(dataContainer.getLocalDataFile());
                HdfsUtils.copyInputStreamToHdfs(yarnConfiguration, dataInputStream, filePath);
            } catch (FileNotFoundException e) {
                log.error(String.format("Cannot find the data container file. Exception: %s, retry attempt = %d",
                        e.toString(), retries));
                fileCopy = false;
            } catch (IOException e) {
                log.error(String.format("Cannot write stream to Hdfs. Exception: %s, retry attempt = %d", e.toString(),
                        retries));
                exception = e;
            }
        } while (exception != null && retries <= MAX_RETRIES);

        if (exception != null && retries > MAX_RETRIES) {
            throw new RuntimeException(
                    String.format("Cannot write stream to Hdfs. Exception: %s", exception.toString()));
        }
        return fileCopy;
    }

    private String generateAvroFileName(String fileName, int fileIndex) {
        int extensionPos = fileName.lastIndexOf('.');
        String name = fileName.substring(0, extensionPos);
        String extension = fileName.substring(extensionPos);
        return String.format("%s_%d%s", name, fileIndex, extension);
    }

    private int getFileIndex(String fileName, String path, Configuration yarnConfiguration) {
        try {
            List<String> files = HdfsUtils.getFilesForDir(yarnConfiguration, path);
            int index = 0;
            if (files.size() == 0) {
                return index;
            }
            int extensionPos = fileName.lastIndexOf('.');
            String name = fileName.substring(0, extensionPos);
            for (String file : files) {
                if (file.startsWith(name)) {
                    index++;
                }
            }
            return index;
        } catch (IOException e) {
            log.error("Cannot get files from extract folder!");
            return 0;
        }
    }
}
