package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
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
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ScoreWorkflowConfiguration;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ScoreWorkflowSubmitter.class);

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private BucketedScoreService bucketedScoreService;

    public ApplicationId submit(ModelSummary modelSummary, String sourceDisplayName,
            TransformationGroup transformationGroup) {

        String modelId = modelSummary.getId();
        String tableToScoreName = modelSummary.getTrainingTableName();

        log.info(String.format(
                "Submitting score workflow for modelId %s and tableToScore %s for customer %s and source %s", modelId,
                tableToScoreName, MultiTenantContext.getCustomerSpace(), sourceDisplayName));

        Table tableToScore = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), tableToScoreName);
        if (tableToScore == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { tableToScoreName });
        }

        if (!modelSummaryService.modelIdinTenant(modelId, MultiTenantContext.getCustomerSpace().toString())) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        Table modelingEventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                modelSummary.getEventTableName());
        if (modelingEventTable == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { modelSummary.getEventTableName() });
        }

        ScoreWorkflowConfiguration configuration = generateConfiguration(modelId, tableToScoreName, modelingEventTable,
                sourceDisplayName, transformationGroup);

        return workflowJobService.submit(configuration);
    }

    public ScoreWorkflowConfiguration generateConfiguration(String modelId, String tableToScoreName,
            Table modelingEventTable, String sourceDisplayName, TransformationGroup transformationGroup) {
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "scoreWorkflow");

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
        if (bucketMetadataList == null) {
            throw new LedpException(LedpCode.LEDP_18128, new String[] { modelId });
        }

        return new ScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .matchClientDocument(matchClientDocument) //
                .microServiceHostPort(microserviceHostPort) //
                .internalResourceHostPort(internalResourceHostPort) //
                .modelId(modelId) //
                .inputTableName(tableToScoreName) //
                .sourceSchemaInterpretation(summary.getSourceSchemaInterpretation()) //
                .excludeDataCloudAttrs(summary.getModelSummaryConfiguration()
                        .getBoolean(ProvenancePropertyName.ExcludePropdataColumns)) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .columnSelection(selection, selectionVersion) //
                .dataCloudVersion(dataCloudVersion) //
                .matchDebugEnabled(!summary.getModelSummaryConfiguration()
                        .getBoolean(ProvenancePropertyName.ExcludePropdataColumns) && plsFeatureFlagService.isMatchDebugEnabled()) //
                .matchRequestSource(MatchRequestSource.SCORING) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename("/"
                        + StringUtils.substringBeforeLast(sourceDisplayName.replaceAll("[^A-Za-z0-9_]", "_"), ".csv")
                        + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .transformationGroup(transformationGroup) //
                .transformDefinitions(getTransformDefinitions(modelingEventTable, transformationGroup))//
                .bucketMetadata(bucketMetadataList) //
                .build();
    }
}
