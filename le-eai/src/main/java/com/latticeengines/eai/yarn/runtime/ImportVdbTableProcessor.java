package com.latticeengines.eai.yarn.runtime;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.yarn.runtime.SingleContainerYarnProcessor;
import com.latticeengines.domain.exposed.eai.EaiImportJobDetail;
import com.latticeengines.domain.exposed.eai.ImportConfiguration;
import com.latticeengines.domain.exposed.eai.ImportContext;
import com.latticeengines.domain.exposed.eai.ImportProperty;
import com.latticeengines.domain.exposed.eai.ImportStatus;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.SourceImportConfiguration;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.pls.VdbLoadTableStatus;
import com.latticeengines.eai.service.EaiImportJobDetailService;
import com.latticeengines.eai.service.EaiMetadataService;
import com.latticeengines.eai.service.ImportService;
import com.latticeengines.remote.exposed.service.DataLoaderService;

@Component("importVdbTableProcessor")
public class ImportVdbTableProcessor extends SingleContainerYarnProcessor<ImportConfiguration> implements
        ItemProcessor<ImportConfiguration, String> {

    private static final Log log = LogFactory.getLog(ImportVdbTableProcessor.class);

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private EaiMetadataService eaiMetadataService;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private EaiImportJobDetailService eaiImportJobDetailService;

    @Override
    public String process(ImportConfiguration importConfig) throws Exception {

        SourceImportConfiguration sourceImportConfiguration = importConfig.getSourceConfigurations().get(0);
        ImportService importService = ImportService.getImportService(sourceImportConfiguration.getSourceType());
        ImportContext importContext = new ImportContext(yarnConfiguration);
        String connectorStr = importConfig.getProperty(ImportProperty.IMPORT_CONFIG_STR);

        String customerSpace = importConfig.getCustomerSpace().toString();
        importContext.setProperty(ImportProperty.CUSTOMER, customerSpace);

        importContext.setProperty(ImportProperty.EXTRACT_PATH, new HashMap<String, String>());
        importContext.setProperty(ImportProperty.PROCESSED_RECORDS, new HashMap<String, Long>());
        VdbConnectorConfiguration vdbConnectorConfiguration = null;
        try {
            log.info("Start getting connector config.");
            vdbConnectorConfiguration = (VdbConnectorConfiguration) importService
                    .generateConnectorConfiguration(connectorStr, importContext);

            try {
                log.info("Initialize import job detail record");
                initJobDetail(vdbConnectorConfiguration);
                log.info("Import metadata");
                List<Table> metadata = importService.importMetadata(sourceImportConfiguration, importContext, vdbConnectorConfiguration);

                metadata = sortTable(metadata, vdbConnectorConfiguration);

                sourceImportConfiguration.setTables(metadata);

                log.info("Import table data");
                importService.importDataAndWriteToHdfs(sourceImportConfiguration, importContext, vdbConnectorConfiguration);

                eaiMetadataService.updateTableSchema(metadata, importContext);
                eaiMetadataService.registerTables(metadata, importContext);

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
        return null;
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
            log.info(String.format("Collection identifier: %s",entry.getValue().getCollectionIdentifier()));
            EaiImportJobDetail jobDetail = eaiImportJobDetailService.getImportJobDetail(entry.getValue()
                    .getCollectionIdentifier());
            if (jobDetail == null) {
                jobDetail = new EaiImportJobDetail();
                jobDetail.setStatus(ImportStatus.SUBMITTED);
                jobDetail.setSourceType(SourceType.VISIDB);
                jobDetail.setCollectionIdentifier(entry.getValue().getCollectionIdentifier());
                jobDetail.setProcessedRecords(0);
                jobDetail.setCollectionTimestamp(new Date());
                if (appId != null) {
                    jobDetail.setLoadApplicationId(appId.toString());
                }
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
            EaiImportJobDetail jobDetail = eaiImportJobDetailService.getImportJobDetail(entry.getValue()
                    .getCollectionIdentifier());
            jobDetail.setStatus(ImportStatus.SUCCESS);
            eaiImportJobDetailService.updateImportJobDetail(jobDetail);
        }
    }

    private void cleanup(VdbConnectorConfiguration config) {
        if (config == null || config.isDlDataReady()) {
            return;
        }
        for (Map.Entry<String, ImportVdbTableConfiguration> entry : config.getTableConfigurations().entrySet()) {
            EaiImportJobDetail jobDetail = eaiImportJobDetailService.getImportJobDetail(entry.getValue()
                    .getCollectionIdentifier());
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
