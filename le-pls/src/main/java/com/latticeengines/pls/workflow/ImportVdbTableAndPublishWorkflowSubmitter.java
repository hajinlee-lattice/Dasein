package com.latticeengines.pls.workflow;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.eai.ImportVdbTableConfiguration;
import com.latticeengines.domain.exposed.eai.VdbConnectorConfiguration;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportVdbTableAndPublishWorkflowConfiguration;
import com.latticeengines.proxy.exposed.eai.EaiJobDetailProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;

@Deprecated
@Component
public class ImportVdbTableAndPublishWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = Logger.getLogger(ImportVdbTableAndPublishWorkflowSubmitter.class);

    @Autowired
    private EaiJobDetailProxy eaiJobDetailProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    public ApplicationId submit(VdbLoadTableConfig loadConfig) {
        String customSpace = CustomerSpace.parse(loadConfig.getTenantId()).toString();
        String collectionIdentifier = String.format("%s_%s_%s", customSpace, loadConfig.getTableName(),
                loadConfig.getLaunchId());

        ImportVdbTableAndPublishWorkflowConfiguration configuration = generateConfiguration(loadConfig, collectionIdentifier);

        log.info(String.format(
                "Submitting import visidb table workflow for tenant: %s. Table name: %s, total rows: %d",
                loadConfig.getTenantId(), loadConfig.getTableName(), loadConfig.getTotalRows()));

        return workflowJobService.submit(configuration);
    }

    public ImportVdbTableAndPublishWorkflowConfiguration generateConfiguration(VdbLoadTableConfig loadConfig,
            String collectionIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(loadConfig.getTenantId());
        VdbConnectorConfiguration vdbConnectorConfiguration = new VdbConnectorConfiguration();
        vdbConnectorConfiguration.setGetQueryDataEndpoint(loadConfig.getGetQueryDataEndpoint());
        vdbConnectorConfiguration.setReportStatusEndpoint(loadConfig.getReportStatusEndpoint());
        vdbConnectorConfiguration.setDlDataReady(true);
        ImportVdbTableConfiguration importVdbTableConfiguration = new ImportVdbTableConfiguration();
        importVdbTableConfiguration.setBatchSize(loadConfig.getBatchSize());
        importVdbTableConfiguration.setDataCategory(loadConfig.getDataCategory());
        importVdbTableConfiguration.setCollectionIdentifier(collectionIdentifier);
        importVdbTableConfiguration.setVdbQueryHandle(loadConfig.getVdbQueryHandle());
        importVdbTableConfiguration.setMergeRule(loadConfig.getMergeRule());
        importVdbTableConfiguration.setCreateTableRule(loadConfig.getCreateTableRule());
        importVdbTableConfiguration.setMetadataList(loadConfig.getMetadataList());
        importVdbTableConfiguration.setTotalRows(loadConfig.getTotalRows());

        vdbConnectorConfiguration.addTableConfiguration(loadConfig.getTableName(), importVdbTableConfiguration);
        String vdbConnectorConfigurationStr = JsonUtils.serialize(vdbConnectorConfiguration);
        return new ImportVdbTableAndPublishWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .collectionIdentifier(collectionIdentifier)
                .importConfigurationStr(vdbConnectorConfigurationStr)
                .build();
    }

}
