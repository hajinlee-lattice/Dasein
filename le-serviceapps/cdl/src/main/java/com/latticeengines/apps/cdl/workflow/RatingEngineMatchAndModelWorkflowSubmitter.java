package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.apps.core.util.UpdateTransformDefinitionsUtils;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.DataRule;
import com.latticeengines.domain.exposed.pls.CloneModelingParameters;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelSummaryProvenanceProperty;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.RatingEngineMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class RatingEngineMatchAndModelWorkflowSubmitter extends WorkflowSubmitter {

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Inject
    private BatonService batonService;

    @Value("${pls.modelingservice.basedir}")
    protected String modelingServiceHdfsBaseDir;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(RatingEngineMatchAndModelWorkflowSubmitter.class);

    public ApplicationId submit(List<String> cloneTableNames, CloneModelingParameters parameters,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary) {
        FeatureFlagValueMap flags = batonService.getFeatureFlags(MultiTenantContext.getCustomerSpace());
        TransformationGroup transformationGroup;
        String originalTransformationGroup = getTransformationGroupNameForModelSummary(modelSummary);
        if (parameters.enableTransformation() && originalTransformationGroup.equals("none")) {
            transformationGroup = FeatureFlagUtils.getTransformationGroupFromZK(flags);
        } else if (parameters.enableTransformation()) {
            transformationGroup = TransformationGroup.fromName(originalTransformationGroup);
        } else {
            transformationGroup = TransformationGroup.NONE;
        }

        RatingEngineMatchAndModelWorkflowConfiguration configuration = generateConfiguration(cloneTableNames,
                parameters, transformationGroup, userRefinedAttributes, modelSummary, flags);
        return workflowJobService.submit(configuration);
    }

    public RatingEngineMatchAndModelWorkflowConfiguration generateConfiguration(List<String> cloneTableNames,
            CloneModelingParameters parameters, TransformationGroup transformationGroup,
            List<Attribute> userRefinedAttributes, ModelSummary modelSummary, FeatureFlagValueMap flags) {

        String sourceSchemaInterpretation = modelSummary.getSourceSchemaInterpretation();
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "ratingEngineModelAndEmailWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, parameters.getDisplayName());
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, cloneTableNames.get(0));

        boolean expectedValue = getExpectedValue(modelSummary);
        List<DataRule> dataRules = parameters.getDataRules();
        if (parameters.getDataRules() == null || parameters.getDataRules().isEmpty()) {
            Table eventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                    modelSummary.getEventTableName());
            dataRules = eventTable.getDataRules();
        }

        String trainingFilePath = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TrainingFilePath, "");

        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(sourceSchemaInterpretation, transformationGroup);

        RatingEngineMatchAndModelWorkflowConfiguration.Builder builder = new RatingEngineMatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .sourceSchemaInterpretation(sourceSchemaInterpretation) //
                .inputProperties(inputProperties) //
                .trainingTableName(cloneTableNames.get(0)) //
                .targetTableName(cloneTableNames.get(1)) //
                .userId(parameters.getUserId()) //
                .transformationGroup(transformationGroup, stdTransformDefns) //
                .enableV2Profiling(FeatureFlagUtils.isV2ProfilingEnabled(flags) || modelSummary
                        .getModelSummaryConfiguration().getBoolean(ProvenancePropertyName.IsV2ProfilingEnabled, false)) //
                .sourceModelSummary(modelSummary) //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupType(parameters.getDeduplicationType()) //
                .matchClientDocument(matchClientDocument) //
                .excludePublicDomain(parameters.isExcludePublicDomain()) //
                .treatPublicDomainAsNormalDomain(false) // TODO: hook up to UI
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN) //
                .fetchOnly(true) //
                .excludeDataCloudAttrs(parameters.isExcludePropDataAttributes()) //
                .skipStandardTransform(!parameters.enableTransformation()) //
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, trainingFilePath) //
                .addProvenanceProperty(ProvenancePropertyName.FuzzyMatchingEnabled,
                        FeatureFlagUtils.isFuzzyMatchEnabled(flags)) //
                .addProvenanceProperty(ProvenancePropertyName.RefineAndCloneParentModelId, modelSummary.getId()) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .dataCloudVersion(getDataCloudVersion(modelSummary.getDataCloudVersion(), flags))//
                .matchColumnSelection(Predefined.getDefaultSelection(), null) //
                .moduleName(modelSummary.getModuleName()) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .skipImport(true) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .pivotArtifactPath(modelSummary.getPivotArtifactPath()) //
                .isDefaultDataRules(false) //
                .dataRules(dataRules) //
                .userRefinedAttributes(userRefinedAttributes) //
                .setRetainLatticeAccountId(true) //
                .setActivateModelSummaryByDefault(parameters.getActivateModelSummaryByDefault()) //
                .cdlModel(true) //
                .setUniqueKeyColumn(InterfaceName.AnalyticPurchaseState_ID.name()) //
                .setEventColumn(InterfaceName.Target.name()) //
                .setExpectedValue(expectedValue) //
                .setUseScorederivation(false) //
                .setModelIdFromRecord(false) //
                .liftChart(false) //
                .aiModelId(parameters.getAiModelId()) //
                .ratingEngineId(parameters.getRatingEngineId()) //
                .notesContent(parameters.getNotesContent());
        return builder.build();
    }

    private boolean getExpectedValue(ModelSummary modelSummary) {
        List<ModelSummaryProvenanceProperty> provenanceProperty = modelSummary.getModelSummaryProvenanceProperties();
        boolean[] result = new boolean[1];
        provenanceProperty.forEach(p -> {
            if (p.getOption().equals("Expected_Value")) {
                result[0] = Boolean.parseBoolean(p.getValue());
            }
        });
        return result[0];
    }

    private String getDataCloudVersion(String dataCloudVersion, FeatureFlagValueMap flags) {
        if (FeatureFlagUtils.useDnBFlagFromZK(flags)) {
            // retrieve latest version from matchapi
            return columnMetadataProxy.latestVersion(dataCloudVersion).getVersion();
        }
        return null;
    }

    private String getTransformationGroupNameForModelSummary(ModelSummary modelSummary) {
        String transformationGroupName = modelSummary.getModelSummaryConfiguration()
                .getString(ProvenancePropertyName.TransformationGroupName, null);
        if (transformationGroupName == null) {
            transformationGroupName = modelSummary.getTransformationGroupName();
        }
        if (transformationGroupName == null) {
            throw new LedpException(LedpCode.LEDP_18108, new String[] { modelSummary.getId() });
        }

        return transformationGroupName;
    }

}
