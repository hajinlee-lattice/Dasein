package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.util.ArtifactUtils;
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
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.modelreview.DataRuleListName;
import com.latticeengines.domain.exposed.modelreview.DataRuleLists;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngineType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class CustomEventModelingWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CustomEventModelingWorkflowSubmitter.class);

    @Inject
    private MetadataProxy metadataProxy;

    @Inject
    private MatchCommandProxy matchCommandProxy;

    @Inject
    private ColumnMetadataProxy columnMetadataProxy;

    @Inject
    private BatonService batonService;

    @Value("${pls.modeling.validation.min.rows:300}")
    private long minRows;

    @Value("${pls.modeling.validation.min.eventrows:50}")
    private long minPositiveEvents;

    @Value("${pls..modeling.validation.min.negativerows:250}")
    private long minNegativeEvents;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    private InternalResourceRestApiProxy internalResourceProxy;

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    private RatingEngineType ratingEngineType;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
        ratingEngineType = RatingEngineType.CUSTOM_EVENT;
    }

    public ApplicationId submit(String customerSpace, ModelingParameters parameters) {

        SourceFile sourceFile = internalResourceProxy.findSourceFileByName(parameters.getFilename(), customerSpace);

        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { parameters.getFilename() });
        }
        FeatureFlagValueMap flags = batonService.getFeatureFlags(MultiTenantContext.getCustomerSpace());

        TransformationGroup transformationGroup = FeatureFlagUtils.getTransformationGroupFromZK(flags);

        if (parameters.getTransformationGroup() == null) {
            parameters.setTransformationGroup(transformationGroup);
        }
        CustomEventModelingWorkflowConfiguration configuration = generateConfiguration(parameters, sourceFile, flags);
        ApplicationId applicationId = workflowJobService.submit(configuration);
        sourceFile.setApplicationId(applicationId.toString());
        internalResourceProxy.updateSourceFile(sourceFile, customerSpace);
        return applicationId;
    }

    private CustomEventModelingWorkflowConfiguration generateConfiguration(ModelingParameters parameters,
            SourceFile sourceFile, FeatureFlagValueMap flags) {

        String trainingTableName = sourceFile.getTableName();

        if (trainingTableName == null) {
            throw new LedpException(LedpCode.LEDP_18099, new String[] { sourceFile.getDisplayName() });
        }

        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getDisplayName() });
        }

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "customEventModelingWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, parameters.getDisplayName());
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceFile.getDisplayName());
        inputProperties.put(WorkflowContextConstants.Inputs.RATING_ENGINE_ID, parameters.getRatingEngineId());
        inputProperties.put(WorkflowContextConstants.Inputs.RATING_MODEL_ID, parameters.getAiModelId());

        Predefined predefinedSelection = Predefined.getDefaultSelection();
        String predefinedSelectionName = parameters.getPredefinedSelectionName();
        if (StringUtils.isNotEmpty(predefinedSelectionName)) {
            predefinedSelection = Predefined.fromName(predefinedSelectionName);
            if (predefinedSelection == null) {
                throw new IllegalArgumentException("Cannot parse column selection named " + predefinedSelectionName);
            }
        }

        Table trainingTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                trainingTableName);

        String moduleName = parameters.getModuleName();
        final String pivotFileName = parameters.getPivotFileName();
        Artifact pivotArtifact = getPivotArtifact(moduleName, pivotFileName);
        if (pivotArtifact != null) {
            trainingTable = ArtifactUtils.getPivotedTrainingTable(pivotArtifact.getPath(), trainingTable,
                    yarnConfiguration);
            metadataProxy.updateTable(MultiTenantContext.getCustomerSpace().toString(), trainingTable.getName(),
                    trainingTable);
        }
        log.debug("Modeling parameters: " + parameters.toString());

        String schemaInterpretation = sourceFile.getSchemaInterpretation().toString();
        TransformationGroup transformationGroup = parameters.getTransformationGroup();
        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(schemaInterpretation, transformationGroup);
        boolean isLPI = CustomEventModelingType.LPI.equals(parameters.getCustomEventModelingType());
        DataCollection.Version version = null;
        if (!isLPI) {
            version = dataCollectionProxy.getActiveVersion(getCustomerSpace().toString());
        }
        return new CustomEventModelingWorkflowConfiguration.Builder() //
                .microServiceHostPort(microserviceHostPort) //
                .customer(getCustomerSpace()) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort) //
                .importReportNamePrefix(sourceFile.getName() + "_Report") //
                .eventTableReportNamePrefix(sourceFile.getName() + "_EventTableReport") //
                .dedupDataFlowBeanName("dedupEventTable") //
                .userId(parameters.getUserId()) //
                .dedupType(parameters.getDeduplicationType()) //
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir) //
                .excludeDataCloudAttrs(parameters.getExcludePropDataColumns()) //
                .keepMatchLid(true) //
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN) //
                .matchDebugEnabled(
                        !parameters.getExcludePropDataColumns() && FeatureFlagUtils.isMatchDebugEnabled(flags)) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .fetchOnly(!isLPI) //
                .skipStandardTransform(parameters.getTransformationGroup() == TransformationGroup.NONE) //
                // null means latest
                .dataCloudVersion(getDataCloudVersion(parameters, flags)) //
                .matchAccountIdColumn(InterfaceName.AccountId.name())
                .modelingType(parameters.getCustomEventModelingType()) //
                .modelName(parameters.getName()) //
                .displayName(parameters.getDisplayName()) //
                .sourceSchemaInterpretation(schemaInterpretation) //
                .trainingTableName(trainingTableName) //
                .inputProperties(inputProperties) //
                .minRows(minRows) //
                .minPositiveEvents(minPositiveEvents) //
                .minNegativeEvents(minNegativeEvents) //
                .transformationGroup(transformationGroup, stdTransformDefns) //
                .enableV2Profiling(FeatureFlagUtils.isV2ProfilingEnabled(flags)) //
                .excludePublicDomains(parameters.isExcludePublicDomains()) //
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, sourceFile.getPath()) //
                .addProvenanceProperty(ProvenancePropertyName.FuzzyMatchingEnabled,
                        FeatureFlagUtils.isFuzzyMatchEnabled(flags)) //
                .pivotArtifactPath(pivotArtifact != null ? pivotArtifact.getPath() : null) //
                .moduleName(moduleName != null ? moduleName : null) //
                .runTimeParams(parameters.runTimeParams) //
                .isDefaultDataRules(true) //
                .dataRules(DataRuleLists.getDataRules(DataRuleListName.STANDARD)) //
                // TODO: legacy SQL based match engine configurations
                .matchClientDocument(matchClientDocument) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .setRetainLatticeAccountId(true) //
                .setActivateModelSummaryByDefault(parameters.getActivateModelSummaryByDefault()) //
                .notesContent(parameters.getNotesContent()) //
                .targetTableName(trainingTableName + "_TargetTable") //
                .skipLdcAttributesOnly(
                        !parameters.isExcludeCDLAttributes() || !parameters.isExcludeCustomFileAttributes()) //
                .aiModelId(parameters.getAiModelId()) //
                .ratingEngineId(parameters.getRatingEngineId()) //
                .setUseScorederivation(false) //
                .setModelIdFromRecord(false) //
                .saveBucketMetadata() //
                .idColumnName(trainingTable.getPrimaryKey().getAttributes().get(0)) //
                .cdlMultiModel(!isLPI) //
                .dataCollectionVersion(version) //
                .setUserRefinedAttributes(parameters.getUserRefinedAttributes()) //
                .modelIteration(parameters.getModelIteration()) //
                .ratingEngineType(ratingEngineType) //
                .build();
    }

    private String getDataCloudVersion(ModelingParameters parameters, FeatureFlagValueMap flags) {
        if (StringUtils.isNotEmpty(parameters.getDataCloudVersion())) {
            return parameters.getDataCloudVersion();
        }
        if (FeatureFlagUtils.useDnBFlagFromZK(flags)) {
            // retrieve latest version from matchapi
            return columnMetadataProxy.latestVersion(null).getVersion();
        }
        return null;
    }

}
