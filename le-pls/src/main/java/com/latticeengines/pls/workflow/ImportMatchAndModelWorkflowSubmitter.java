package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.latticeengines.domain.exposed.datacloud.MatchClientDocument;
import com.latticeengines.domain.exposed.datacloud.MatchCommandType;
import com.latticeengines.domain.exposed.datacloud.match.MatchRequestSource;
import com.latticeengines.domain.exposed.dataflow.flows.leadprioritization.DedupType;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Artifact;
import com.latticeengines.domain.exposed.metadata.ArtifactType;
import com.latticeengines.domain.exposed.metadata.Attribute;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.modeling.PivotValuesLookup;
import com.latticeengines.domain.exposed.modelreview.DataRuleListName;
import com.latticeengines.domain.exposed.modelreview.DataRuleLists;
import com.latticeengines.domain.exposed.pls.ModelingParameters;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.util.ModelingUtils;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndModelWorkflowConfiguration;
import com.latticeengines.pls.service.MetadataFileUploadService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.pls.util.PivotMappingFileUtils;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ImportMatchAndModelWorkflowSubmitter extends BaseModelWorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ImportMatchAndModelWorkflowSubmitter.class);

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private MetadataFileUploadService metadataFileUploadService;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    @Value("${pls.modeling.validation.min.rows:300}")
    private long minRows;

    @Value("${pls.modeling.validation.min.eventrows:50}")
    private long minPositiveEvents;

    @Value("${pls..modeling.validation.min.negativerows:250}")
    private long minNegativeEvents;

    public ImportMatchAndModelWorkflowConfiguration generateConfiguration(ModelingParameters parameters) {

        SourceFile sourceFile = sourceFileService.findByName(parameters.getFilename());

        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { parameters.getFilename() });
        }

        String trainingTableName = sourceFile.getTableName();

        if (trainingTableName == null) {
            throw new LedpException(LedpCode.LEDP_18099, new String[] { sourceFile.getDisplayName() });
        }

        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getDisplayName() });
        }

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importMatchAndModelWorkflow");
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
        Artifact pivotArtifact = null;

        if (StringUtils.isNotEmpty(moduleName) && StringUtils.isNotEmpty(pivotFileName)) {
            List<Artifact> pivotArtifacts = metadataFileUploadService.getArtifacts(moduleName,
                    ArtifactType.PivotMapping);
            pivotArtifact = Iterables.find(pivotArtifacts, new Predicate<Artifact>() {
                @Override
                public boolean apply(Artifact artifact) {
                    return artifact.getName().equals(pivotFileName);
                }
            }, null);
            if (pivotFileName != null && pivotArtifact == null) {
                throw new LedpException(LedpCode.LEDP_28026, new String[] { pivotFileName, moduleName });
            }
        }
        if (pivotArtifact != null) {
            updateTrainingTable(pivotArtifact.getPath(), trainingTable);
        }
        log.info("Modeling parameters: " + parameters.toString());

        ImportMatchAndModelWorkflowConfiguration.Builder builder = new ImportMatchAndModelWorkflowConfiguration.Builder()
                .microServiceHostPort(microserviceHostPort)
                .customer(getCustomerSpace())
                .sourceFileName(sourceFile.getName())
                .matchInputTableName(sourceFile.getTableName())
                .sourceType(SourceType.FILE)
                .internalResourceHostPort(internalResourceHostPort)
                .importReportNamePrefix(sourceFile.getName() + "_Report")
                .eventTableReportNamePrefix(sourceFile.getName() + "_EventTableReport")
                .dedupDataFlowBeanName("dedupEventTable")
                .userId(parameters.getUserId())
                .dedupType(parameters.getDeduplicationType())
                .modelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir)
                .excludeDataCloudAttrs(parameters.getExcludePropDataColumns())
                .skipDedupStep(parameters.getDeduplicationType() == DedupType.MULTIPLELEADSPERDOMAIN)
                .matchDebugEnabled(!parameters.getExcludePropDataColumns() && plsFeatureFlagService.isMatchDebugEnabled())
                .matchRequestSource(MatchRequestSource.MODELING)
                .skipStandardTransform(parameters.getTransformationGroup() == TransformationGroup.NONE)
                .matchColumnSelection(predefinedSelection, parameters.getSelectedVersion())
                // null means latest
                .dataCloudVersion(getDataCloudVersion(parameters))
                .modelName(parameters.getName())
                .displayName(parameters.getDisplayName())
                .sourceSchemaInterpretation(sourceFile.getSchemaInterpretation().toString())
                .trainingTableName(trainingTableName)
                .inputProperties(inputProperties)
                .minRows(minRows)
                .minPositiveEvents(minPositiveEvents)
                .minNegativeEvents(minNegativeEvents)
                .transformationGroup(parameters.getTransformationGroup())
                .enableV2Profiling(plsFeatureFlagService.isV2ProfilingEnabled())
                .excludePublicDomains(parameters.isExcludePublicDomains())
                .addProvenanceProperty(ProvenancePropertyName.TrainingFilePath, sourceFile.getPath())
                .addProvenanceProperty(ProvenancePropertyName.FuzzyMatchingEnabled,
                        plsFeatureFlagService.isFuzzyMatchEnabled())
                .pivotArtifactPath(pivotArtifact != null ? pivotArtifact.getPath() : null)
                .moduleName(moduleName != null ? moduleName : null).runTimeParams(parameters.runTimeParams)
                .isDefaultDataRules(true).dataRules(DataRuleLists.getDataRules(DataRuleListName.STANDARD))
                .bucketMetadata(null)
                // TODO: legacy SQL based match engine configurations
                .matchClientDocument(matchClientDocument).matchType(MatchCommandType.MATCH_WITH_UNIVERSE)
                .matchDestTables("DerivedColumnsCache")
                .notesContent(parameters.getNotesContent());
        return builder.build();
    }

    private String getDataCloudVersion(ModelingParameters parameters) {
        if (StringUtils.isNotEmpty(parameters.getDataCloudVersion())) {
            return parameters.getDataCloudVersion();
        }
        if (plsFeatureFlagService.useDnBFlagFromZK()) {
            // retrieve latest version from matchapi
            return columnMetadataProxy.latestVersion(null).getVersion();
        }
        return null;
    }

    public ApplicationId submit(ModelingParameters parameters) {
        SourceFile sourceFile = sourceFileService.findByName(parameters.getFilename());

        TransformationGroup transformationGroup = plsFeatureFlagService.getTransformationGroupFromZK();

        if (parameters.getTransformationGroup() == null) {
            parameters.setTransformationGroup(transformationGroup);
        }

        ImportMatchAndModelWorkflowConfiguration configuration = generateConfiguration(parameters);

        ApplicationId applicationId = workflowJobService.submit(configuration);
        sourceFile.setApplicationId(applicationId.toString());
        sourceFileService.update(sourceFile);
        return applicationId;
    }

    private void updateTrainingTable(String pivotArtifactPath, Table trainingTable) {

        List<Attribute> trainingAttrs = trainingTable.getAttributes();
        PivotValuesLookup pivotValues = null;
        try {
            pivotValues = ModelingUtils.getPivotValues(yarnConfiguration, pivotArtifactPath);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_00002, e);
        }
        if (pivotValues == null) {
            throw new RuntimeException("PivotValuesLookup is null.");
        }
        Set<String> sourceColumnNames = pivotValues.pivotValuesBySourceColumn.keySet();
        List<Attribute> attrs = PivotMappingFileUtils.createAttrsFromPivotSourceColumns(sourceColumnNames,
                trainingAttrs);

        trainingTable.setAttributes(attrs);
        metadataProxy.updateTable(MultiTenantContext.getCustomerSpace().toString(), trainingTable.getName(),
                trainingTable);
    }
}
