package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.scoring.RTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class RTSBulkScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(RTSBulkScoreWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private BucketedScoreService bucketedScoreService;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    @Value("${pls.modeling.workflow.mem.mb}")
    protected int workflowMemMb;

    public ApplicationId submit(String modelId, String tableToScore, boolean enableLeadEnrichment,
            String sourceDisplayName, boolean enableDebug) {
        log.info(String.format(
                "Submitting rts bulk score workflow for modelId %s and tableToScore %s for customer %s and source %s",
                modelId, tableToScore, MultiTenantContext.getCustomerSpace(), sourceDisplayName));
        if (metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), tableToScore) == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { tableToScore });
        }

        if (!modelSummaryProxy.modelIdinTenant(MultiTenantContext.getTenant().getId(), modelId)) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        RTSBulkScoreWorkflowConfiguration configuration = generateConfiguration(modelId, tableToScore,
                sourceDisplayName, enableLeadEnrichment, enableDebug);

        return workflowJobService.submit(configuration);
    }

    public RTSBulkScoreWorkflowConfiguration generateConfiguration(String modelId, String tableToScore,
            String sourceDisplayName, boolean enableLeadEnrichment, boolean enableDebug) {
        ModelSummary modelSummary = modelSummaryProxy.findByModelId(MultiTenantContext.getTenant().getId(),
                modelId, false, true, false);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "rtsBulkScoreWorkflow");
        if (modelSummary != null) {
            inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelSummary.getDisplayName());
        }

        String dataCloudVersion = getLatestDataCloudVersion();
        boolean skipIdMatch = true;
        if (modelSummary != null) {
            skipIdMatch = !modelSummary.isMatch();
        }
        String modelType = modelSummary != null ? modelSummary.getModelType() : ModelType.PYTHONMODEL.getModelType();
        enableLeadEnrichment = enableLeadEnrichment && !modelType.equals(ModelType.PMML.getModelType());
        skipIdMatch = skipIdMatch && !enableLeadEnrichment;

        log.info("Data Cloud Version=" + dataCloudVersion);
        List<BucketMetadata> bucketMetadataList = bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
        if (bucketMetadataList == null) {
            throw new LedpException(LedpCode.LEDP_18128, new String[] { modelId });
        }

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);
        return new RTSBulkScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .internalResourceHostPort(internalResourceHostPort) //
                .modelId(modelId) //
                .modelType(modelType) //
                .sourceSchemaInterpretation(modelSummary.getSourceSchemaInterpretation()) //
                .inputTableName(tableToScore) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename("/"
                        + StringUtils.substringBeforeLast(sourceDisplayName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv")
                        + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .enableLeadEnrichment(enableLeadEnrichment) //
                .setScoreTestFile(false) //
                .enableDebug(enableDebug) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("AccountMasterColumn") //
                .matchColumnSelection(Predefined.ID, "1.0.0") //
                .dataCloudVersion(dataCloudVersion) //
                .excludeDataCloudAttrs(skipIdMatch) //
                .skipMatching(ModelType.PMML.getModelType().equals(modelType)) //
                .matchDebugEnabled(!skipIdMatch && !ModelType.PMML.getModelType().equals(modelType)
                        && plsFeatureFlagService.isMatchDebugEnabled()) //
                .matchRequestSource(MatchRequestSource.ENRICHMENT) //
                .matchClientDocument(matchClientDocument) //
                .bucketMetadata(bucketMetadataList) //
                .matchQueue(LedpQueueAssigner.getScoringQueueNameForSubmission()) //
                .workflowContainerMem(workflowMemMb) //
                .build();
    }
}
