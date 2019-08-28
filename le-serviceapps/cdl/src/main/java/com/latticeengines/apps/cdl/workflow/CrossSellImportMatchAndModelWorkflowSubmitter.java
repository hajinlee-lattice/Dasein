package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ProxyResourceService;
import com.latticeengines.apps.core.util.FeatureFlagUtils;
import com.latticeengines.apps.core.util.UpdateTransformDefinitionsUtils;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.cdl.CrossSellModelingParameters;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modelreview.DataRuleListName;
import com.latticeengines.domain.exposed.modelreview.DataRuleLists;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.CrossSellImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class CrossSellImportMatchAndModelWorkflowSubmitter extends AbstractModelWorkflowSubmitter {
    private static final Logger log = LoggerFactory.getLogger(CrossSellImportMatchAndModelWorkflowSubmitter.class);

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Autowired
    protected MetadataProxy metadataProxy;

    @Inject
    private ProxyResourceService proxyResourceService;

    @Autowired
    protected BatonService batonService;

    @Value("${pls.modelingservice.basedir}")
    protected String modelingServiceHdfsBaseDir;

    @Value("${cdl.modeling.workflow.mem.mb}")
    protected int workflowMemMb;

    private RatingEngineType ratingEngineType = RatingEngineType.CROSS_SELL;

    public CrossSellImportMatchAndModelWorkflowConfiguration generateConfiguration(
            CrossSellModelingParameters parameters) {
        FeatureFlagValueMap flags = getFeatureFlagValueMap();

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "CrossSellImportMatchAndModelWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, parameters.getDisplayName());
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, parameters.getName());
        inputProperties.put(WorkflowContextConstants.Inputs.RATING_ENGINE_ID, parameters.getRatingEngineId());
        inputProperties.put(WorkflowContextConstants.Inputs.RATING_MODEL_ID, parameters.getAiModelId());
        ColumnSelection.Predefined predefinedSelection = ColumnSelection.Predefined.getDefaultSelection();
        String predefinedSelectionName = parameters.getPredefinedSelectionName();
        if (StringUtils.isNotEmpty(predefinedSelectionName)) {
            predefinedSelection = ColumnSelection.Predefined.fromName(predefinedSelectionName);
            if (predefinedSelection == null) {
                throw new IllegalArgumentException("Cannot parse column selection named " + predefinedSelectionName);
            }
        }

        String moduleName = parameters.getModuleName();
        final String pivotFileName = parameters.getPivotFileName();
        Artifact pivotArtifact = getPivotArtifact(moduleName, pivotFileName);
        log.debug("Modeling parameters: " + parameters.toString());

        TransformationGroup transformationGroup = parameters.getTransformationGroup();
        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(SchemaInterpretation.SalesforceAccount.toString(), transformationGroup);
        String tableName = getTableName(parameters);

        Table table = metadataProxy.getTable(getCustomerSpace().toString(), tableName);
        String eventColumnName = InterfaceName.Target.name();
        if (table != null && CollectionUtils.isNotEmpty(table.getAttributes(LogicalDataType.Event))) {
            eventColumnName = table.getAttributes(LogicalDataType.Event).get(0).getName();
        }
        boolean targetScoreDerivationEnabled = FeatureFlagUtils.isTargetScoreDerivation(flags);

        String targetTableName = tableName + "_TargetTable";
        DataCollection.Version version = proxyResourceService.getDataCollection(getCustomerSpace().toString()).getVersion();
        CrossSellImportMatchAndModelWorkflowConfiguration.Builder builder = new CrossSellImportMatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .filterTableNames(parameters.getTrainFilterTableName(), parameters.getEventFilterTableName(),
                        parameters.getTargetFilterTableName()) //
                .filterQueries(parameters.getTrainFilterQuery(), parameters.getEventFilterQuery(),
                        parameters.getTargetFilterQuery()) //
                .internalResourceHostPort(internalResourceHostPort) //
                .userId(parameters.getUserId()) //
                .dedupDataFlowBeanName("dedupEventTable") //
                .dedupType(parameters.getDeduplicationType()) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .excludeDataCloudAttrs(parameters.getExcludePropDataColumns()) //
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN) //
                .fetchOnly(true) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .skipStandardTransform(transformationGroup == TransformationGroup.NONE) //
                .matchColumnSelection(predefinedSelection, parameters.getSelectedVersion()) //
                // null means latest
                .dataCloudVersion(getDataCloudVersion(parameters)) //
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .sourceSchemaInterpretation(SchemaInterpretation.SalesforceAccount.toString()) //
                .trainingTableName(tableName) //
                .targetTableName(targetTableName) //
                .inputProperties(inputProperties) //
                .transformationGroup(transformationGroup, stdTransformDefns) //
                .enableV2Profiling(isV2ProfilingEnabled()) //
                .excludePublicDomains(parameters.isExcludePublicDomains()) //
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, getTrainPath(parameters)) //
                .addProvenanceProperty(ProvenancePropertyName.FuzzyMatchingEnabled, true) //
                .addProvenanceProperty(ProvenancePropertyName.IsV2ProfilingEnabled, isV2ProfilingEnabled()) //
                // TODO: plsFeatureFlagService.isFuzzyMatchEnabled()) //
                .pivotArtifactPath(pivotArtifact != null ? pivotArtifact.getPath() : null) //
                .moduleName(moduleName) //
                .isDefaultDataRules(true) //
                .dataRules(DataRuleLists.getDataRules(DataRuleListName.STANDARD)) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .setRetainLatticeAccountId(true) //
                .setActivateModelSummaryByDefault(parameters.getActivateModelSummaryByDefault()) //
                .crossSellModel(true) //
                .setUniqueKeyColumn(InterfaceName.__Composite_Key__.name()) //
                .cdlMultiModel(true) //
                .setEventColumn(eventColumnName) //
                .idColumnName(InterfaceName.AnalyticPurchaseState_ID.name()) //
                .matchGroupId(InterfaceName.AccountId.name()) //
                .setExpectedValue(parameters.isExpectedValue()) //
                .setUseScorederivation(false) //
                .dataCollectionVersion(version) //
                .aiModelId(parameters.getAiModelId()) //
                .ratingEngineId(parameters.getRatingEngineId()) //
                .setUserRefinedAttributes(parameters.getUserRefinedAttributes()) //
                .modelIteration(parameters.getModelIteration()) //
                .workflowContainerMem(workflowMemMb) //
                .notesContent(parameters.getNotesContent()) //
                .targetScoreDerivationEnabled(targetScoreDerivationEnabled) //
                .ratingEngineType(ratingEngineType) //
                .apsRollupPeriod(proxyResourceService
                        .getOrCreateDataCollectionStatus(getCustomerSpace().toString(), version).getApsRollingPeriod());
        return builder.build();
    }

    private String getTableName(CrossSellModelingParameters parameters) {
        String tableName = parameters.getTableName();
        if (StringUtils.isNotEmpty(tableName)) {
            return tableName;
        }
        return "RatingEngineModel_" + System.currentTimeMillis();
    }

    private String getTrainPath(CrossSellModelingParameters parameters) {
        String outputFileName = "file_" + getTableName(parameters) + ".csv";
        return PathBuilder.buildDataFilePath(CamilleEnvironment.getPodId(), getCustomerSpace()).toString() + "/"
                + outputFileName;
    }

    private String getDataCloudVersion(ModelingParameters parameters) {
        if (StringUtils.isNotEmpty(parameters.getDataCloudVersion())) {
            return parameters.getDataCloudVersion();
        }
        if (true) {
            // TODO: if (plsFeatureFlagService.useDnBFlagFromZK()) {
            return columnMetadataProxy.latestVersion(null).getVersion();
        }
        return null;
    }

    public ApplicationId submit(CrossSellModelingParameters parameters) {
        TransformationGroup transformationGroup = TransformationGroup.NONE; // TODO:
                                                                            // plsFeatureFlagService.getTransformationGroupFromZK();
        if (parameters.getTransformationGroup() == null) {
            parameters.setTransformationGroup(transformationGroup);
        }
        CrossSellImportMatchAndModelWorkflowConfiguration configuration = generateConfiguration(parameters);
        return workflowJobService.submit(configuration);
    }
}
