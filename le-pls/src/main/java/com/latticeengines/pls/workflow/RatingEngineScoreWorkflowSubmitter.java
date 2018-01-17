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
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.MatchJoinType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class RatingEngineScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(RatingEngineScoreWorkflowSubmitter.class);

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private BucketedScoreService bucketedScoreService;

    public ApplicationId submit(ModelSummary modelSummary, String sourceDisplayName, EventFrontEndQuery targetQuery,
            String tableToScoreName) {
        String modelId = modelSummary.getId();
        log.info(String.format(
                "Submitting score workflow for modelId %s and tableToScore %s for customer %s and source %s", modelId,
                tableToScoreName, MultiTenantContext.getCustomerSpace(), sourceDisplayName));

        if (!modelSummaryService.modelIdinTenant(modelId, MultiTenantContext.getCustomerSpace().toString())) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        RatingEngineScoreWorkflowConfiguration configuration = generateConfiguration(modelId, targetQuery,
                tableToScoreName, sourceDisplayName);
        return workflowJobService.submit(configuration);
    }

    public RatingEngineScoreWorkflowConfiguration generateConfiguration(String modelId, EventFrontEndQuery targetQuery,
            String tableToScoreName, String sourceDisplayName) {
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "ratingEngineScoreWorkflow");

        ModelSummary summary = modelSummaryService.findByModelId(modelId, false, true, false);
        if (summary != null) {
            inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, summary.getDisplayName());
        }
        Predefined selection = Predefined.getLegacyDefaultSelection();
        String selectionVersion = null;
        if (summary != null && summary.getPredefinedSelection() != null) {
            selection = summary.getPredefinedSelection();
            if (StringUtils.isNotEmpty(summary.getPredefinedSelectionVersion())) {
                selectionVersion = summary.getPredefinedSelectionVersion();
            }
        }
        String dataCloudVersion = getComplatibleDataCloudVersionFromModelSummary(summary);

        List<BucketMetadata> bucketMetadataList = bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
        // if (bucketMetadataList == null) {
        // throw new LedpException(LedpCode.LEDP_18128, new String[] { modelId
        // });
        // }

        return new RatingEngineScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .matchClientDocument(matchClientDocument) //
                .microServiceHostPort(microserviceHostPort) //
                .internalResourceHostPort(internalResourceHostPort) //
                .modelId(modelId) //
                .inputTableName("RatingEngineTarget_" + System.currentTimeMillis()) //
                .filterTableName(tableToScoreName) //
                .filterQuery(targetQuery) //
                .sourceSchemaInterpretation(summary.getSourceSchemaInterpretation()) //
                .excludeDataCloudAttrs(summary.getModelSummaryConfiguration()
                        .getBoolean(ProvenancePropertyName.ExcludePropdataColumns)) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .columnSelection(selection, selectionVersion) //
                .dataCloudVersion(dataCloudVersion) //
                .matchRequestSource(MatchRequestSource.SCORING) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename("/"
                        + StringUtils.substringBeforeLast(sourceDisplayName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv")
                        + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .bucketMetadata(bucketMetadataList) //
                .matchQueue(LedpQueueAssigner.getScoringQueueNameForSubmission()) //
                .setUniqueKeyColumn(InterfaceName.AnalyticPurchaseState_ID.name()) //
                .setUseScorederivation(false) //
                .setEventColumn(InterfaceName.Target.name()) //
                .build();
    }
}
