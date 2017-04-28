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
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ProvenancePropertyName;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndScoreWorkflowConfiguration;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component
public class ImportMatchAndScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = Logger.getLogger(ImportMatchAndScoreWorkflowSubmitter.class);

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private ModelSummaryService modelSummaryService;

    @Autowired
    private BucketedScoreService bucketedScoreService;

    public ApplicationId submit(ModelSummary modelSummary, String fileName, TransformationGroup transformationGroup) {
        SourceFile sourceFile = sourceFileService.findByName(fileName);
        String modelId = modelSummary.getId();
        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { fileName });
        }

        Table table = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                sourceFile.getTableName());
        if (table == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { sourceFile.getTableName() });
        }

        if (!modelSummaryService.modelIdinTenant(modelId, MultiTenantContext.getCustomerSpace().toString())) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getDisplayName() });
        }

        WorkflowConfiguration configuration = generateConfiguration(modelId, sourceFile, sourceFile.getDisplayName(),
                transformationGroup);

        log.info(String.format(
                "Submitting testing data score workflow for modelId %s and tableToScore %s for customer %s and source %s",
                modelId, sourceFile.getTableName(), MultiTenantContext.getCustomerSpace(),
                sourceFile.getDisplayName()));
        ApplicationId applicationId = workflowJobService.submit(configuration);
        sourceFile.setApplicationId(applicationId.toString());
        sourceFileService.update(sourceFile);
        return applicationId;

    }

    public ImportMatchAndScoreWorkflowConfiguration generateConfiguration(String modelId, SourceFile sourceFile,
            String sourceDisplayName, TransformationGroup transformationGroup) {

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);
        ModelSummary modelSummary = modelSummaryService.findByModelId(modelId, false, true, false);

        Table modelingEventTable = metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                modelSummary.getEventTableName());
        if (modelingEventTable == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { modelSummary.getEventTableName() });
        }

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importMatchAndScoreWorkflow");
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelSummary.getDisplayName());

        Predefined selection = Predefined.getLegacyDefaultSelection();
        String selectionVersion = null;
        if (modelSummary != null) {
            if (modelSummary.getPredefinedSelection() != null) {
                selection = modelSummary.getPredefinedSelection();
                if (StringUtils.isNotEmpty(modelSummary.getPredefinedSelectionVersion())) {
                    selectionVersion = modelSummary.getPredefinedSelectionVersion();
                }
            }
        }
        String dataCloudVersion = getComplatibleDataCloudVersionFromModelSummary(modelSummary);
        String sourceFileDisplayName = sourceFile.getDisplayName() != null ? sourceFile.getDisplayName() : "unnamed";

        List<BucketMetadata> bucketMetadataList = bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
        if (bucketMetadataList == null) {
            throw new LedpException(LedpCode.LEDP_18128, new String[] { modelId });
        }

        return new ImportMatchAndScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort)//
                .reportNamePrefix(sourceFile.getName() + "_Report") //
                .modelId(modelId) //
                .sourceSchemaInterpretation(modelSummary.getSourceSchemaInterpretation()) //
                .inputTableName(sourceFile.getTableName()) //
                .excludeDataCloudAttrs(modelSummary.getModelSummaryConfiguration()
                        .getBoolean(ProvenancePropertyName.ExcludePropdataColumns)) //
                .matchClientDocument(matchClientDocument) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .matchColumnSelection(selection, selectionVersion) //
                .dataCloudVersion(dataCloudVersion) //
                .matchDebugEnabled(plsFeatureFlagService.isMatchDebugEnabled()) //
                .matchRequestSource(MatchRequestSource.SCORING) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename(
                        "/" + StringUtils.substringBeforeLast(sourceFileDisplayName.replaceAll("[^A-Za-z0-9_]", "_"),
                                ".csv") + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .internalResourcePort(internalResourceHostPort) //
                .transformationGroup(transformationGroup) //
                .transformDefinitions(getTransformDefinitions(modelingEventTable, transformationGroup)) //
                .bucketMetadata(bucketMetadataList) //
                .build();
    }
}
