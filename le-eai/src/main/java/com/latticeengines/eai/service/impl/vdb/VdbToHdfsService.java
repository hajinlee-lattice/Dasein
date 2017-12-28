package com.latticeengines.eai.service.impl.vdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.VdbToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("vdbToHdfsService")
public class VdbToHdfsService extends EaiRuntimeService<VdbToHdfsConfiguration> {

    private static Logger log = LoggerFactory.getLogger(VdbToHdfsService.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void invoke(VdbToHdfsConfiguration config) {

        try {
            SourceImportConfiguration sourceImportConfiguration = config.getSourceConfigurations().get(0);
            ImportService importService = ImportService.getImportService(sourceImportConfiguration.getSourceType());
            ImportContext importContext = new ImportContext(yarnConfiguration);
            String connectorStr = config.getProperty(ImportProperty.IMPORT_CONFIG_STR);

            String customerSpace = config.getCustomerSpace().toString();
            importContext.setProperty(ImportProperty.CUSTOMER, customerSpace);

            importContext.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
            importContext.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
            importContext.setProperty(ImportProperty.MULTIPLE_EXTRACT, new HashMap<String, Boolean>());
            importContext.setProperty(ImportProperty.EXTRACT_PATH_LIST, new HashMap<String, List<String>>());
            importContext.setProperty(ImportProperty.EXTRACT_RECORDS_LIST, new HashMap<String, List<Long>>());
            importContext.setProperty(ImportProperty.IGNORED_ROWS, new HashMap<String, Long>());
            importContext.setProperty(ImportProperty.IGNORED_ROWS_LIST, new HashMap<String, List<Long>>());
            importContext.setProperty(ImportProperty.DUPLICATE_ROWS, new HashMap<String, Long>());
            importContext.setProperty(ImportProperty.DUPLICATE_ROWS_LIST, new HashMap<String, List<Long>>());
            importContext.setProperty(ImportProperty.BUSINESS_ENTITY, config.getBusinessEntity());
            String collectionIdentifiers = config.getProperty(ImportProperty.COLLECTION_IDENTIFIERS);
            if (!StringUtils.isEmpty(collectionIdentifiers)) {
                @SuppressWarnings("unchecked")
                List<Object> identifiersRaw = JsonUtils.deserialize(collectionIdentifiers, List.class);
                List<String> identifiers = JsonUtils.convertList(identifiersRaw, String.class);

                VdbConnectorConfiguration vdbConnectorConfiguration = null;
                try {
                    log.info("Start getting connector config.");
                    vdbConnectorConfiguration = (VdbConnectorConfiguration) importService
                            .generateConnectorConfiguration(connectorStr, importContext);

                    try {
                        log.info("Initialize import job detail record");
                        initJobDetail(vdbConnectorConfiguration);
                        log.info("Import metadata");
                        HashMap<String, Table> tableTemplates = getTableMap(config.getCustomerSpace().toString(),
                                identifiers);

                        List<Table> metadata = importService.prepareMetadata(new ArrayList<>(tableTemplates.values()));
                        metadata = sortTable(metadata, vdbConnectorConfiguration);

                        sourceImportConfiguration.setTables(metadata);

                        log.info("Import table data");
                        importService.importDataAndWriteToHdfs(sourceImportConfiguration, importContext,
                                vdbConnectorConfiguration);

                        log.info("Finalize import job detail record");
                        finalizeJobDetail(vdbConnectorConfiguration, tableTemplates, importContext);
                    } catch (Exception e) {
                        throw e;
                    }
                } catch (LedpException e) {
                    switch (e.getCode()) {
                    case LEDP_17011:
                    case LEDP_17012:
                    case LEDP_17013:
                        log.error("Generate connector configuration error!");
                        break;
                    default:
                        break;
                    }

                } catch (Exception e) {
                    throw e;
                }
            }
        } catch (Exception e) {
            updateJobDetailStatus("", ImportStatus.FAILED);
            throw e;
        }

    }

    private HashMap<String, Table> getTableMap(String customerSpace, List<String> taskIds) {
        HashMap<String, Table> tables = new HashMap<>();
        for (String taskId : taskIds) {
            DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
            if (dataFeedTask != null) {
                tables.put(taskId, dataFeedTask.getImportTemplate());
            }
        }
        return tables;
    }

    private List<Table> sortTable(List<Table> tables, VdbConnectorConfiguration config) {
        List<Table> result = new ArrayList<>();
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            for (Table table : tables) {
                if (table.getName().equals(entry.getKey())) {
                    result.add(table);
                    break;
                }
            }
        }
        return result;
    }

    private void initJobDetail(VdbConnectorConfiguration config) {
        log.info(String.format("Table config count: %d", config.getTableConfigurations().size()));
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            log.info(String.format("Collection identifier: %s", entry.getValue().getCollectionIdentifier()));
            initJobDetail(entry.getValue().getCollectionIdentifier(), SourceType.VISIDB);
        }
    }

    @SuppressWarnings("unchecked")
    private void finalizeJobDetail(VdbConnectorConfiguration config, HashMap<String, Table> tableMetaData,
            ImportContext importContext) {
        Map<String, Boolean> multipleExtractMap = importContext.getProperty(ImportProperty.MULTIPLE_EXTRACT, Map.class);
        Map<String, String> targetPathsMap = importContext.getProperty(ImportProperty.EXTRACT_PATH, Map.class);
        Map<String, Long> processedRecordsMap = importContext.getProperty(ImportProperty.PROCESSED_RECORDS, Map.class);
        Map<String, List<String>> multipleTargets = importContext.getProperty(ImportProperty.EXTRACT_PATH_LIST,
                Map.class);
        Map<String, List<Long>> multipleRecords = importContext.getProperty(ImportProperty.EXTRACT_RECORDS_LIST,
                Map.class);
        Map<String, Long> ignoredRecord = importContext.getProperty(ImportProperty.IGNORED_ROWS, Map.class);
        Map<String, List<Long>> mutipleIgnoredRecords = importContext.getProperty(ImportProperty.IGNORED_ROWS_LIST,
                Map.class);

        Map<String, Long> duplicateRecord = importContext.getProperty(ImportProperty.DUPLICATE_ROWS, Map.class);
        Map<String, List<Long>> mutipleDuplicatedRecords = importContext.getProperty(ImportProperty.DUPLICATE_ROWS_LIST,
                Map.class);
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            Table table = tableMetaData.get(entry.getValue().getCollectionIdentifier());
            Long totalRows = (long) entry.getValue().getTotalRows();
            if (multipleExtractMap.get(table.getName())) {
                List<String> recordList = new ArrayList<>();
                for (Long record : multipleRecords.get(table.getName())) {
                    recordList.add(record.toString());
                }
                Long ignoredRows = 0L;
                for (Long record : mutipleIgnoredRecords.get(table.getName())) {
                    ignoredRows += record;
                }
                Long duplicateRows = 0L;
                for (Long record : mutipleDuplicatedRecords.get(table.getName())) {
                    duplicateRows += record;
                }
                updateJobDetailExtractInfo(entry.getValue().getCollectionIdentifier(), table.getName(),
                        multipleTargets.get(table.getName()), recordList, totalRows, ignoredRows, duplicateRows);
            } else {
                Long ignoredRows = ignoredRecord.get(table.getName());
                Long duplicateRows = duplicateRecord.get(table.getName());
                updateJobDetailExtractInfo(entry.getValue().getCollectionIdentifier(), table.getName(),
                        Arrays.asList(targetPathsMap.get(table.getName())),
                        Arrays.asList(processedRecordsMap.get(table.getName()).toString()),
                        totalRows, ignoredRows, duplicateRows);
            }
        }

    }
}
