package com.latticeengines.eai.service.impl.vdb;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.NamingUtils;
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
import com.latticeengines.domain.exposed.pls.VdbSpecMetadata;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.TableUtils;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.eai.service.impl.vdb.converter.VdbTableToAvroTypeConverter;
import com.latticeengines.eai.service.impl.vdb.converter.VdbValueConverter;
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

    @Value("${eai.vdb.extract.size:1000000}")
    private int recordsPerExtract;

    @Value("${eai.vdb.transaction.batch.size:100000}")
    private int transactionBatchSize;

    @Value("${eai.vdb.batch.size:50000}")
    private int batchSize;

    @Autowired
    @Qualifier("commonTaskExecutor")
    private ThreadPoolTaskExecutor taskExecutor;

    private VdbValueConverter vdbValueConverter = new VdbValueConverter();

    private static final int MAX_RETRIES = 3;

    private Set<String> uniqueIds = Collections.synchronizedSet(new HashSet<>());

    private Map<Integer, VdbDataProcessCallable> callableMap = new HashMap<>();
    private Map<Integer, String> targetPathMap = new HashMap<>();
    private Set<String> pathSet = new HashSet<>();
    private Map<Integer, Future<Integer[]>> processFutureMap = new HashMap<>();

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
            targetPath = generateNewTargetPath(customerSpace, null, BusinessEntity.Account);
        }
        return targetPath;
    }

    private String generateNewTargetPath(CustomerSpace customerSpace, Configuration yarnConfiguration,
                                         BusinessEntity businessEntity) {
        String targetPath = String.format("%s/%s/DataFeed1/DataFeed1-%s/Extracts/%s",
                PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                SourceType.VISIDB.getName(), businessEntity.name(),
                new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date()));
        int j = 1;
        while (pathSet.contains(targetPath)) {
            targetPath = String.format("%s/%s/DataFeed1/DataFeed1-%s/Extracts/%s",
                    PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                    SourceType.VISIDB.getName(), businessEntity.name(),
                    new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date(System.currentTimeMillis() + j * 1000L)));
            j++;
        }
        if (yarnConfiguration != null) {
            try {
                int i = 1;
                while(HdfsUtils.fileExists(yarnConfiguration, targetPath)) {
                    targetPath = String.format("%s/%s/DataFeed1/DataFeed1-%s/Extracts/%s",
                            PathBuilder.buildDataTablePath(CamilleEnvironment.getPodId(), customerSpace).toString(),
                            SourceType.VISIDB.getName(), businessEntity.name(),
                            new SimpleDateFormat(EXTRACT_DATE_FORMAT).format(new Date(System.currentTimeMillis() +
                                    i * 1000L)));
                    i++;
                }
            } catch (IOException e) {
                log.error("Cannot access hdfs!");
            }
        }
        pathSet.add(targetPath);
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
                        .convertTypeToAvro(attribute.getPhysicalDataType().toLowerCase()).name());
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
            context.getProperty(ImportProperty.IGNORED_ROWS_LIST, Map.class).put(table.getName(),
                    new ArrayList<Long>());
            context.getProperty(ImportProperty.DUPLICATE_ROWS_LIST, Map.class).put(table.getName(),
                    new ArrayList<Long>());
        } else {
            context.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class).put(table.getName(), Boolean.FALSE);
        }
    }

    @SuppressWarnings("unchecked")
    public void updateExtractContext(Table table, ImportContext context, String targetPath, Long processedRecords,
                                     Long errorRecords, Long duplicateRecords) {
        Configuration config = context.getProperty(ImportProperty.HADOOPCONFIG, Configuration.class);
        String defaultFS = config.get(FileSystem.FS_DEFAULT_NAME_KEY);
        String hdfsUri = String.format("%s%s/%s", defaultFS, targetPath, "*.avro");
        List.class.cast(context.getProperty(ImportVdbProperty.EXTRACT_PATH_LIST, Map.class).get(table.getName()))
                .add(hdfsUri);
        List.class.cast(context.getProperty(ImportVdbProperty.EXTRACT_RECORDS_LIST, Map.class).get(table.getName()))
                .add(processedRecords);
        List.class.cast(context.getProperty(ImportVdbProperty.IGNORED_ROWS_LIST, Map.class).get(table.getName()))
                .add(errorRecords);
        List.class.cast(context.getProperty(ImportVdbProperty.DUPLICATE_ROWS_LIST, Map.class).get(table.getName()))
                .add(duplicateRecords);
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
        BusinessEntity businessEntity = context.getProperty(ImportProperty.BUSINESS_ENTITY, BusinessEntity.class);
        List<Table> tables = extractionConfig.getTables();
        for (Table table : tables) {
            ImportVdbTableConfiguration importVdbTableConfiguration = vdbConnectorConfiguration
                    .getVdbTableConfiguration(table.getName());
            if (importVdbTableConfiguration == null) {
                continue;
            }

            long startTime = System.currentTimeMillis();
            try {
                String queryDataUrl = vdbConnectorConfiguration.getGetQueryDataEndpoint();
                String statusUrl = vdbConnectorConfiguration.getReportStatusEndpoint();
                String queryHandle = importVdbTableConfiguration.getVdbQueryHandle();
                VdbLoadTableStatus vdbLoadTableStatus = new VdbLoadTableStatus();
                vdbLoadTableStatus.setVdbQueryHandle(queryHandle);
                String avroFileName = String.format("Extract_%s.avro", table.getName());
                String extractIdentifier = importVdbTableConfiguration.getCollectionIdentifier();
                Configuration yarnConfiguration = context.getProperty(ImportVdbProperty.HADOOPCONFIG,
                        Configuration.class);
                int totalRows = importVdbTableConfiguration.getTotalRows();
                int rowsToGet = getBatchSize(businessEntity);
                setExtractContextForVdbTable(table, context, importVdbTableConfiguration, true);
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
                if (processedLines > 0) {
                    importJobDetail.setProcessedRecords(0);
                }
                int startRow = 0;

                VdbGetQueryData vdbGetQueryData = new VdbGetQueryData();
                vdbGetQueryData.setVdbQueryHandle(queryHandle);
                vdbGetQueryData.setStartRow(startRow);
                vdbGetQueryData.setRowsToGet(rowsToGet);

                vdbLoadTableStatus.setJobStatus("Running");
                vdbLoadTableStatus.setMessage("Start import data for Vdb table");
                reportStatus(statusUrl, vdbLoadTableStatus);
                long getDLDataResultTime = 0;
                int retryForEachBatch = 0;
                while (startRow < totalRows) {
                    VdbQueryDataResult vdbQueryDataResult = null;
                    try {
                        long readDLStartTime = System.currentTimeMillis();
                        vdbQueryDataResult = dataLoaderService.getQueryDataResult(queryDataUrl,
                                vdbGetQueryData);
                        getDLDataResultTime += System.currentTimeMillis() - readDLStartTime;
                        if (vdbQueryDataResult == null) {
                            log.error("Failed to get data from DL!");
                            throw new LedpException(LedpCode.LEDP_17016);
                        }
                        if (vdbQueryDataResult.isSuccess()) {
                            retryForEachBatch = 0;
                            String dataFileName = String.format("DL_RAW_DATA_%d", startRow);
                            FileUtils.writeStringToFile(new File(dataFileName),
                                    JsonUtils.serialize(vdbQueryDataResult), Charset.defaultCharset());

                            String targetPath = getTargetPath(startRow, customerSpace, yarnConfiguration,
                                    businessEntity);
                            VdbDataProcessCallable callable = getDataProcessCallable(startRow, avroFileName,
                                    targetPath, businessEntity != BusinessEntity.Transaction, table,
                                    yarnConfiguration);
                            callable.addDataFile(dataFileName);
                            log.info(String.format("Add %s datafile in queue. ", dataFileName));
                            startRow += rowsToGet;
                            if (startRow > totalRows) {
                                startRow = totalRows;
                            }
                            vdbGetQueryData.setStartRow(startRow);
                            vdbLoadTableStatus.setJobStatus("Running");
                            vdbLoadTableStatus.setMessage(
                                    String.format("%d of total %d records are processed.", startRow, totalRows));
                            vdbLoadTableStatus.setProcessedRecords(startRow);

                            reportStatus(statusUrl, vdbLoadTableStatus);
                        } else {
                            if (retryForEachBatch < MAX_RETRIES) {
                                retryForEachBatch++;
                            } else {
                                log.error("Failed to get data from DL!");
                                throw new LedpException(LedpCode.LEDP_17016);
                            }
                        }
                    } catch (Exception e) {
                        log.error(String.format("Errors occur when get data from DL! Exception: %s",
                                e.toString()));
                        throw new LedpException(LedpCode.LEDP_17016);
                    }
                }
                log.info(String.format("Get data from DL takes %d ms", getDLDataResultTime));

                for (Map.Entry<Integer, VdbDataProcessCallable> callableEntry : callableMap.entrySet()) {
                    callableEntry.getValue().setStop(true);
                }
                try {
                    for (Map.Entry<Integer, Future<Integer[]>> futureEntry : processFutureMap.entrySet()) {
                        Integer[] result = futureEntry.getValue().get();
                        updateExtractContext(table, context, targetPathMap.get(futureEntry.getKey()),
                                new Long(result[0]), new Long(result[1]), new Long(result[2]));
                    }
                } catch (InterruptedException e) {
                    log.error("Importing thread is terminated!");
                    throw new RuntimeException("Importing thread is terminated!");
                } catch (ExecutionException e) {
                    log.error("Error importing data to avro file!");
                    throw new RuntimeException(e);
                }

                importJobDetail.setProcessedRecords(totalRows);
                importJobDetail.setStatus(ImportStatus.SUCCESS);
                eaiImportJobDetailService.updateImportJobDetail(importJobDetail);
                log.info(String.format("Total load data from DL takes %d ms",
                        System.currentTimeMillis() - startTime));
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

    private int getBatchSize(BusinessEntity businessEntity) {
        switch (businessEntity) {
            case Transaction:
                return transactionBatchSize;
            case Account:
            case Contact:
            case Product:
                return batchSize;
            default:
                return 20000;
        }
    }

    private String getTargetPath(int startRow, CustomerSpace customerSpace, Configuration yarnConfiguration,
                                 BusinessEntity businessEntity) {
        int index = startRow / recordsPerExtract;
        if (targetPathMap.containsKey(index)) {
            return targetPathMap.get(index);
        } else {
            String targetPath = generateNewTargetPath(customerSpace, yarnConfiguration, businessEntity);
            targetPathMap.put(index, targetPath);
            return targetPath;
        }
    }

    private VdbDataProcessCallable getDataProcessCallable(int startRow, String avroFileName, String extractPath,
                                                      boolean needDedup, Table table, Configuration yarnConfiguration) {
        int index = startRow / recordsPerExtract;
        if (callableMap.containsKey(index)) {
            return callableMap.get(index);
        } else {
            if (callableMap.get(index - 1) != null) {
                callableMap.get(index - 1).setStop(true);
            }
            VdbDataProcessCallable callable = getDataProcessCallable(avroFileName, extractPath, needDedup, table,
                    yarnConfiguration);
            callableMap.put(index, callable);
            processFutureMap.put(index, taskExecutor.submit(callable));
            return callable;
        }
    }

    private VdbDataProcessCallable getDataProcessCallable(String avroFileName, String extractPath, boolean needDedup,
                                                        Table table, Configuration yarnConfiguration) {
        VdbDataProcessCallable.Builder builder = new VdbDataProcessCallable.Builder();
        Queue<String> fileQueue = new ConcurrentLinkedDeque<>();
        builder.processorId(NamingUtils.uuid("VdbDataProcessor"))
                .stop(false)
                .avroFileName(avroFileName)
                .extractPath(extractPath)
                .fileQueue(fileQueue)
                .needDedup(needDedup)
                .table(table)
                .uniqueIds(uniqueIds)
                .vdbValueConverter(new VdbValueConverter())
                .yarnConfiguration(yarnConfiguration);
        VdbDataProcessCallable callable = new VdbDataProcessCallable(builder);
        return callable;
    }
}
