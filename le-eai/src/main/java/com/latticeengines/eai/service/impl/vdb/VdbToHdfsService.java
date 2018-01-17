package com.latticeengines.eai.service.impl.vdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
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
import com.latticeengines.domain.exposed.eai.ImportVdbTableMergeRule;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.VdbToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.proxy.exposed.cdl.CDLProxy;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("vdbToHdfsService")
public class VdbToHdfsService extends EaiRuntimeService<VdbToHdfsConfiguration> {

    private static Logger log = LoggerFactory.getLogger(VdbToHdfsService.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Autowired
    private CDLProxy cdlProxy;

    @Override
    @SuppressWarnings("unchecked")
    public void invoke(VdbToHdfsConfiguration config) {
        String jobDetailIds = config.getProperty(ImportProperty.EAIJOBDETAILIDS);
        List<Object> jobDetailIdsRaw = JsonUtils.deserialize(jobDetailIds, List.class);
        List<Long> eaiJobDetailIds = JsonUtils.convertList(jobDetailIdsRaw, Long.class);
        Long jobDetailId = eaiJobDetailIds.size() > 0 ? eaiJobDetailIds.get(0) : -1L;

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

            VdbConnectorConfiguration vdbConnectorConfiguration = null;
            try {
                log.info("Start getting connector config.");
                vdbConnectorConfiguration = (VdbConnectorConfiguration) importService
                        .generateConnectorConfiguration(connectorStr, importContext);

                LinkedHashMap<String, ImportVdbTableConfiguration> importVdbTableConfigurationMap =
                        vdbConnectorConfiguration.getTableConfigurations();
                if(importVdbTableConfigurationMap.size() <= 0) {
                    throw new LedpException(LedpCode.LEDP_17011, new String[] { "No import vdb table configuration"
                    });
                }
                ImportVdbTableMergeRule mergeRule = importVdbTableConfigurationMap.entrySet().iterator().next().getValue().getMergeRule();

                try {
                    log.info("Initialize import job detail record");
                    initJobDetail(jobDetailId, vdbConnectorConfiguration);
                    log.info("Import metadata");
                    HashMap<Long, Table> tableTemplates = getTableMap(config.getCustomerSpace().toString(),
                            eaiJobDetailIds);

                    List<Table> metadata = importService.prepareMetadata(new ArrayList<>(tableTemplates.values()));
                    metadata = sortTable(metadata, vdbConnectorConfiguration);

                    sourceImportConfiguration.setTables(metadata);

                    log.info("Import table data");
                    importService.importDataAndWriteToHdfs(sourceImportConfiguration, importContext,
                            vdbConnectorConfiguration);

                    log.info("Finalize import job detail record");
                    finalizeJobDetail(vdbConnectorConfiguration, tableTemplates, importContext);

                    if(mergeRule == ImportVdbTableMergeRule.REPLACE) {
                        ApplicationId applicationId = cdlProxy.cleanupAllData(config.getCustomerSpace().toString(),
                                config.getBusinessEntity());

                        waitForWorkflowStatus(applicationId.toString(), false);
                    }

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
                throw e;

            } catch (Exception e) {
                throw e;
            }
        } catch (Exception e) {
            updateJobDetailStatus(jobDetailId, ImportStatus.FAILED);
            throw e;
        }

    }

    private HashMap<Long, Table> getTableMap(String customerSpace, List<Long> jobDetailIds) {
        HashMap<Long, Table> tables = new HashMap<>();
        for (Long jobId : jobDetailIds) {
            String taskId = getTaskIdFromJobId(jobId);
            if (taskId != null) {
                DataFeedTask dataFeedTask = dataFeedProxy.getDataFeedTask(customerSpace, taskId);
                if (dataFeedTask != null) {
                    tables.put(jobId, dataFeedTask.getImportTemplate());
                }
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

    private void initJobDetail(Long jobDetailId, VdbConnectorConfiguration config) {
        log.info(String.format("Table config count: %d", config.getTableConfigurations().size()));
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            log.info(String.format("Collection identifier: %s", entry.getValue().getCollectionIdentifier()));
            initJobDetail(jobDetailId, entry.getValue().getCollectionIdentifier(), SourceType.VISIDB);
        }
    }

    @SuppressWarnings("unchecked")
    private void finalizeJobDetail(VdbConnectorConfiguration config, HashMap<Long, Table> tableMetaData,
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
        for (Map.Entry<Long, Table> entry : tableMetaData.entrySet()) {
            Table table = entry.getValue();
            Long totalRows = (long) config.getVdbTableConfiguration(table.getName()).getTotalRows();
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
                updateJobDetailExtractInfo(entry.getKey(), table.getName(),
                        multipleTargets.get(table.getName()), recordList, totalRows, ignoredRows, duplicateRows);
            } else {
                Long ignoredRows = ignoredRecord.get(table.getName());
                Long duplicateRows = duplicateRecord.get(table.getName());
                updateJobDetailExtractInfo(entry.getKey(), table.getName(),
                        Arrays.asList(targetPathsMap.get(table.getName())),
                        Arrays.asList(processedRecordsMap.get(table.getName()).toString()),
                        totalRows, ignoredRows, duplicateRows);
            }
        }

    }
}
