package com.latticeengines.eai.service.impl.vdb;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.eai.VdbToHdfsConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Extract;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;
import com.latticeengines.eai.runtime.service.EaiRuntimeService;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.proxy.exposed.metadata.DataFeedProxy;

@Component("vdbToHdfsService")
public class VdbToHdfsService extends EaiRuntimeService<VdbToHdfsConfiguration> {

    private static Logger log = LoggerFactory.getLogger(VdbToHdfsService.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Autowired
    private DataFeedProxy dataFeedProxy;

    @Override
    public void invoke(VdbToHdfsConfiguration config) {

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

                    Map<String, List<Extract>> extracts = eaiMetadataService.getExtractsForTable(metadata,
                            importContext);
                    for (String taskId : identifiers) {
                        List<Extract> extract = extracts.get(tableTemplates.get(taskId).getName());
                        if (extract != null && extract.size() > 0) {
                            if (extract.size() == 1) {
                                dataFeedProxy.registerExtract(config.getCustomerSpace().toString(), taskId,
                                        tableTemplates.get(taskId).getName(), extract.get(0));
                            } else {
                                dataFeedProxy.registerExtracts(config.getCustomerSpace().toString(), taskId,
                                        tableTemplates.get(taskId).getName(), extract);
                            }
                        }
                    }
                    log.info("Finalize import job detail record");
                    finalizeJobDetail(vdbConnectorConfiguration);
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
                    cleanup(vdbConnectorConfiguration);
                    break;
                }

            } catch (Exception e) {
                throw e;
            }
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
            EaiImportJobDetail jobDetail = eaiImportJobDetailService
                    .getImportJobDetailByCollectionIdentifier(entry.getValue().getCollectionIdentifier());
            if (jobDetail == null) {
                jobDetail = new EaiImportJobDetail();
                jobDetail.setStatus(ImportStatus.SUBMITTED);
                jobDetail.setSourceType(SourceType.VISIDB);
                jobDetail.setCollectionIdentifier(entry.getValue().getCollectionIdentifier());
                jobDetail.setProcessedRecords(0);
                jobDetail.setCollectionTimestamp(new Date());
                jobDetail.setTargetPath(entry.getValue().getExtractPath());
                eaiImportJobDetailService.createImportJobDetail(jobDetail);
            } else {
                jobDetail.setStatus(ImportStatus.SUBMITTED);
                eaiImportJobDetailService.updateImportJobDetail(jobDetail);
            }

        }
    }

    private void finalizeJobDetail(VdbConnectorConfiguration config) {
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            EaiImportJobDetail jobDetail = eaiImportJobDetailService
                    .getImportJobDetailByCollectionIdentifier(entry.getValue().getCollectionIdentifier());
            if (jobDetail != null) {
                eaiImportJobDetailService.deleteImportJobDetail(jobDetail);
            }
        }
    }

    private void cleanup(VdbConnectorConfiguration config) {
        if (config == null || config.isDlDataReady()) {
            return;
        }
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            EaiImportJobDetail jobDetail = eaiImportJobDetailService
                    .getImportJobDetailByCollectionIdentifier(entry.getValue().getCollectionIdentifier());
            try {
                if (HdfsUtils.fileExists(yarnConfiguration, jobDetail.getTargetPath())) {
                    HdfsUtils.rmdir(yarnConfiguration, jobDetail.getTargetPath());
                }
            } catch (IOException e) {
                log.error("Cannot remove extract dir in hdfs");
            }
            eaiImportJobDetailService.deleteImportJobDetail(jobDetail);
        }
    }
}
