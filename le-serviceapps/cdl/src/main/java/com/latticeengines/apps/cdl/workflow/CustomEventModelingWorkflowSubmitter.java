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
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.CustomEventModelingType;
import com.latticeengines.domain.exposed.modelreview.DataRuleListName;
import com.latticeengines.domain.exposed.modelreview.DataRuleLists;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExportType;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.scoringapi.TransformDefinition;
import com.latticeengines.domain.exposed.serviceflows.cdl.CustomEventModelingWorkflowConfiguration;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.util.SegmentExportUtil;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
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

    @Inject
    private RatingEngineProxy ratingEngineProxy;

    @Value("${pls.modeling.validation.min.rows:300}")
    private long minRows;

    @Value("${pls.modeling.validation.min.eventrows:50}")
    private long minPositiveEvents;

    @Value("${pls..modeling.validation.min.negativerows:250}")
    private long minNegativeEvents;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Value("${pls.modelingservice.basedir}")
    private String modelingServiceHdfsBaseDir;

    private InternalResourceRestApiProxy internalResourceProxy;

    @PostConstruct
    public void init() {
        internalResourceProxy = new InternalResourceRestApiProxy(internalResourceHostPort);
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
        log.info("Modeling parameters: " + parameters.toString());

        String schemaInterpretation = sourceFile.getSchemaInterpretation().toString();
        TransformationGroup transformationGroup = parameters.getTransformationGroup();
        List<TransformDefinition> stdTransformDefns = UpdateTransformDefinitionsUtils
                .getTransformDefinitions(schemaInterpretation, transformationGroup);

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
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN) //
                .matchDebugEnabled(
                        !parameters.getExcludePropDataColumns() && FeatureFlagUtils.isMatchDebugEnabled(flags)) //
                .matchRequestSource(MatchRequestSource.MODELING) //
                .matchQueue(LedpQueueAssigner.getModelingQueueNameForSubmission()) //
                .fetchOnly(true) //
                .skipStandardTransform(parameters.getTransformationGroup() == TransformationGroup.NONE) //
                .matchColumnSelection(predefinedSelection, parameters.getSelectedVersion()) //
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
                .metadataSegmentExport(createMetadataSegmentExport(parameters.getRatingEngineId(),
                        parameters.getCustomEventModelingType())) //
                .targetTableName(trainingTableName + "_TargetTable") //
                .skipLdcAttributesOnly(
                        !parameters.isExcludeCDLAttributes() || !parameters.isExcludeCustomFileAttributes()) //
                .aiModelId(parameters.getAiModelId()) //
                .ratingEngineId(parameters.getRatingEngineId()) //
                .idColumnName(trainingTable.getPrimaryKey().getAttributes().get(0)) //
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

    private MetadataSegmentExport createMetadataSegmentExport(String ratingEngineId, CustomEventModelingType type) {
        if (StringUtils.isEmpty(ratingEngineId) && type.equals(CustomEventModelingType.LPI)) {
            return null;
        }
        CustomerSpace customerSpace = CustomerSpace.parse(MultiTenantContext.getTenant().getId());

        RatingEngine re = ratingEngineProxy.getRatingEngine(customerSpace.toString(), ratingEngineId);
        MetadataSegment segment = re.getSegment();

        if (segment == null) {
            switch (type) {
            case CDL:
                throw new LedpException(LedpCode.LEDP_40018, new String[] { ratingEngineId });
            case LPI:
                return null;
            default:
                throw new LedpException(LedpCode.LEDP_40019, new String[] { type.name() });
            }
        } else {
            MetadataSegmentExport metadataSegmentExport = new MetadataSegmentExport();
            metadataSegmentExport.setAccountFrontEndRestriction(segment.getAccountFrontEndRestriction());
            metadataSegmentExport.setContactFrontEndRestriction(segment.getContactFrontEndRestriction());

            metadataSegmentExport.setType(MetadataSegmentExportType.ACCOUNT_ID);
            String exportedFileName = SegmentExportUtil.constructFileName(metadataSegmentExport.getExportPrefix(), null,
                    metadataSegmentExport.getType());
            metadataSegmentExport.setFileName(exportedFileName);
            Table segmentExportTable = SegmentExportUtil.constructSegmentExportTable(MultiTenantContext.getTenant(),
                    metadataSegmentExport.getType(), exportedFileName);

            metadataProxy.createTable(customerSpace.toString(), segmentExportTable.getName(), segmentExportTable);
            segmentExportTable = metadataProxy.getTable(customerSpace.toString(), segmentExportTable.getName());
            metadataSegmentExport.setTableName(segmentExportTable.getName());

            String path = PathBuilder.buildDataFileUniqueExportPath(CamilleEnvironment.getPodId(), customerSpace)
                    .append("/").toString();
            metadataSegmentExport.setPath(path);
            return metadataSegmentExport;
        }
    }

}
