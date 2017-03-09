package com.latticeengines.pls.workflow;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.VdbImportExtract;
import com.latticeengines.domain.exposed.metadata.VdbImportStatus;
import com.latticeengines.domain.exposed.pls.VdbCreateTableRule;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;
import com.latticeengines.leadprioritization.workflow.ImportVdbTableAndPublishWorkflowConfiguration;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class ImportVdbTableAndPublishWorkflowSubmitter extends WorkflowSubmitter {
    private static final Logger log = Logger.getLogger(ImportVdbTableAndPublishWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    public ApplicationId submit(VdbLoadTableConfig loadConfig) {
        String customSpace = CustomerSpace.parse(loadConfig.getTenantId()).toString();
        String extractIdentifier = String.format("%s_%s_%d", customSpace, loadConfig.getTableName(),
                loadConfig.getLaunchId());
        VdbImportExtract vdbImportExtract = metadataProxy.getVdbImportExtract(customSpace, extractIdentifier);
        if (vdbImportExtract != null) {
            switch (vdbImportExtract.getStatus()) {
                case SUBMITTED:
                    throw new LedpException(LedpCode.LEDP_18136);
                case RUNNING:
                    throw new LedpException(LedpCode.LEDP_18137, new String[]{vdbImportExtract.getLoadApplicationId()});
                case SUCCESS:
                    throw new LedpException(LedpCode.LEDP_18138);
            }
        } else {
            vdbImportExtract = new VdbImportExtract();
            vdbImportExtract.setExtractIdentifier(extractIdentifier);
            vdbImportExtract.setProcessedRecords(0);
            vdbImportExtract.setStatus(VdbImportStatus.SUBMITTED);
            vdbImportExtract.setExtractionTimestamp(new Date());
            metadataProxy.createVdbImportExtract(customSpace, vdbImportExtract);
        }

        ImportVdbTableAndPublishWorkflowConfiguration configuration = generateConfiguration(loadConfig, extractIdentifier);

        log.info(String.format(
                "Submitting import visidb table workflow for tenant: %s. Table name: %s, total rows: %d",
                loadConfig.getTenantId(), loadConfig.getTableName(), loadConfig.getTotalRows()));

        return workflowJobService.submit(configuration);
    }

    public ImportVdbTableAndPublishWorkflowConfiguration generateConfiguration(VdbLoadTableConfig loadConfig,
            String extractIdentifier) {
        CustomerSpace customerSpace = CustomerSpace.parse(loadConfig.getTenantId());
        return new ImportVdbTableAndPublishWorkflowConfiguration.Builder()
                .customer(customerSpace)
                .dataCatagory(loadConfig.getDataCategory())
                .extractIdentifier(extractIdentifier)
                .getQueryDataEndpoint(loadConfig.getGetQueryDataEndpoint())
                .reportStatusEndpoint(loadConfig.getReportStatusEndpoint())
                .vdbQueryHandle(loadConfig.getVdbQueryHandle())
                .tableName(loadConfig.getTableName())
                .totalRows(loadConfig.getTotalRows())
                .metadataList(loadConfig.getMetadataList())
                .mergeRule(loadConfig.getMergeRule())
                .createTableRule(VdbCreateTableRule.getCreateRule(loadConfig.getCreateTableRule()))
                .build();
    }
}
