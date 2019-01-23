package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.MatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.PlsFeatureFlagService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.UpdateTransformDefinitionsUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class MatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private PlsFeatureFlagService plsFeatureFlagService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(MatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(String cloneTableName, CloneModelingParameters parameters,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        TransformationGroup transformationGroup;
        String originalTransformationGroup = getTransformationGroupNameForModelSummary(modelSummary);
        if (parameters.enableTransformation() && originalTransformationGroup.equals("none")) {
            transformationGroup = plsFeatureFlagService.getTransformationGroupFromZK();
        } else if (parameters.enableTransformation()) {
            transformationGroup = TransformationGroup.fromName(originalTransformationGroup);
        } else {
            transformationGroup = TransformationGroup.NONE;
        }

        MatchAndModelWorkflowConfiguration configuration = generateConfiguration(cloneTableName, parameters,
                transformationGroup, userRefinedAttributes, modelSummary);
        return workflowJobService.submit(configuration);
    }

    public MatchAndModelWorkflowConfiguration generateConfiguration(String cloneTableName,
            CloneModelingParameters parameters, TransformationGroup transformationGroup,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        String sourceSchemaInterpretation = modelSummary.getSourceSchemaInterpretation();
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);
        SourceFile sourceFile = sourceFileService.findByTableName(cloneTableName);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "modelAndEmailWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, parameters.getDisplayName());
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME,
                sourceFile != null ? sourceFile.getDisplayName() : cloneTableName);

        Table eventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                modelSummary.getEventTableName());
        String eventColumnName = InterfaceName.Event.name();
        if (CollectionUtils.isNotEmpty(eventTable.getAttributes(LogicalDataType.Event))) {
            eventColumnName = eventTable.getAttributes(LogicalDataType.Event).get(0).getName();
        }

        List<DataRule> dataRules = parameters.getDataRules();
        if (parameters.getDataRules() == null || parameters.getDataRules().isEmpty()) {
            dataRules = eventTable.getDataRules();
        }

        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");

        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(sourceSchemaInterpretation, transformationGroup);

        MatchAndModelWorkflowConfiguration.Builder builder = new MatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(sourceSchemaInterpretation) //
                .inputProperties(inputProperties) //
                .trainingTableName(cloneTableName) //
                .userId(parameters.getUserId()) //
                .transformationGroup(transformationGroup, stdTransformDefns) //
                .enableV2Profiling(plsFeatureFlagService.isV2ProfilingEnabled() || modelSummary
                        .getModelSummaryConfiguration().getBoolean(ProvenancePropertyName.IsV2ProfilingEnabled, false)) //
                .sourceModelSummary(modelSummary) //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupType(parameters.getDeduplicationType()) //
                .matchClientDocument(matchClientDocument) //
                .excludePublicDomain(parameters.isExcludePublicDomain()) //
                .treatPublicDomainAsNormalDomain(false) // TODO: hook up to UI
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN)
                .excludeDataCloudAttrs(parameters.isExcludePropDataAttributes()) //
                .skipStandardTransform(!parameters.enableTransformation()) //
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, trainingFilePath) //
                .addProvenanceProperty(ProvenancePropertyName.FuzzyMatchingEnabled,
                        plsFeatureFlagService.isFuzzyMatchEnabled()) //
                .addProvenanceProperty(ProvenancePropertyName.RefineAndCloneParentModelId, modelSummary.getId()) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .dataCloudVersion(getDataCloudVersion(modelSummary.getDataCloudVersion()))//
                .matchColumnSelection(Predefined.getDefaultSelection(), null) //
                .moduleName(modelSummary.getModuleName()) //
                .matchDebugEnabled(
                        !parameters.isExcludePropDataAttributes() && plsFeatureFlagService.isMatchDebugEnabled()) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .pivotArtifactPath(modelSummary.getPivotArtifactPath()) //
                .isDefaultDataRules(false) //
                .dataRules(dataRules) //
                .eventColumn(eventColumnName) //
                .userRefinedAttributes(userRefinedAttributes) //
                .enableDebug(false) //
                .enableLeadEnrichment(false) //
                .setScoreTestFile(false) //
                .setRetainLatticeAccountId(true) //
                .setActivateModelSummaryByDefault(parameters.getActivateModelSummaryByDefault()) //
                .notesContent(parameters.getNotesContent());
        return builder.build();
    }

    private String getDataCloudVersion(String dataCloudVersion) {
        if (plsFeatureFlagService.useDnBFlagFromZK()) {
            // retrieve latest version from matchapi
            return columnMetadataProxy.latestVersion(dataCloudVersion).getVersion();
        }
        return null;
    }

}
