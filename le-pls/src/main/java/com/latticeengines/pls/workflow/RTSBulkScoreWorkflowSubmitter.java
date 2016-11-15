package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class RTSBulkScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(RTSBulkScoreWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryService modelSummaryService;
    
    @Autowired
    private MatchCommandProxy matchCommandProxy;
    
    public ApplicationId submit(String modelId, String tableToScore, boolean enableLeadEnrichment,
            String sourceDisplayName, boolean enableDebug) {
        log.info(String.format(
                "Submitting rts bulk score workflow for modelId %s and tableToScore %s for customer %s and source %s",
                modelId, tableToScore, MultiTenantContext.getCustomerSpace(), sourceDisplayName));
        RTSBulkScoreWorkflowConfiguration configuration = generateConfiguration(modelId, tableToScore,
                sourceDisplayName, enableLeadEnrichment, enableDebug);

        if (metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), tableToScore) == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { tableToScore });
        }

        if (!modelSummaryService.modelIdinTenant(modelId, MultiTenantContext.getCustomerSpace().toString())) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        return workflowJobService.submit(configuration);
    }

    public RTSBulkScoreWorkflowConfiguration generateConfiguration(String modelId, String tableToScore,
            String sourceDisplayName, boolean enableLeadEnrichment, boolean enableDebug) {

        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, true, false);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "rtsBulkScoreWorkflow");
        if (modelSummary != null) {
            inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelSummary.getDisplayName());
        }

        String dataCloudVersion = null;
        if (modelSummary != null) {
            dataCloudVersion = modelSummary.getDataCloudVersion();
        }
        log.info("Data Cloud Version=" + dataCloudVersion);
        
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);
        return new RTSBulkScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .internalResourcePort(internalResourceHostPort) //
                .modelId(modelId) //
                .inputTableName(tableToScore) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename(
                        "/"
                                + StringUtils.substringBeforeLast(sourceDisplayName.replaceAll("[^A-Za-z0-9_]", "_"),
                                        ".csv") + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .enableLeadEnrichment(enableLeadEnrichment) //
                .enableDebug(enableDebug) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("AccountMasterColumn") //
                .columnSelection(Predefined.ID, "1.0.0") //
                .dataCloudVersion(dataCloudVersion)
                .matchClientDocument(matchClientDocument) //
                .build();
    }
}
