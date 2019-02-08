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
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportAndRTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.pls.service.BucketedScoreService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.lp.ModelSummaryProxy;
import com.latticeengines.proxy.exposed.matchapi.MatchCommandProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

@Component
public class ImportAndRTSBulkScoreWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(ImportAndRTSBulkScoreWorkflowSubmitter.class);

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SourceFileService sourceFileService;

    @Autowired
    private MatchCommandProxy matchCommandProxy;

    @Autowired
    private BucketedScoreService bucketedScoreService;

    @Autowired
    private ModelSummaryProxy modelSummaryProxy;

    @Value("${pls.modeling.workflow.mem.mb}")
    protected int workflowMemMb;

    public ApplicationId submit(String modelId, String fileName, boolean enableLeadEnrichment, boolean enableDebug) {
        SourceFile sourceFile = sourceFileService.findByName(fileName);

        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { fileName });
        }

        if (metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(),
                sourceFile.getTableName()) == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { sourceFile.getTableName() });
        }

        if (!modelSummaryProxy.modelIdinTenant(MultiTenantContext.getTenant().getId(), modelId)) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        if (hasRunningWorkflow(sourceFile)) {
            throw new LedpException(LedpCode.LEDP_18081, new String[] { sourceFile.getDisplayName() });
        }

        WorkflowConfiguration configuration = generateConfiguration(modelId, sourceFile, sourceFile.getDisplayName(),
                enableLeadEnrichment, enableDebug);

        log.info(String.format(
                "Submitting testing data rts bulk score workflow for modelId %s and tableToScore %s for customer %s and source %s",
                modelId, sourceFile.getTableName(), MultiTenantContext.getCustomerSpace(),
                sourceFile.getDisplayName()));
        ApplicationId applicationId = workflowJobService.submit(configuration);
        sourceFile.setApplicationId(applicationId.toString());
        sourceFileService.update(sourceFile);
        return applicationId;
    }

    public ImportAndRTSBulkScoreWorkflowConfiguration generateConfiguration(String modelId, SourceFile sourceFile,
            String sourceDisplayName, boolean enableLeadEnrichment, boolean enableDebug) {

        ModelSummary modelSummary = modelSummaryProxy.findByModelId(MultiTenantContext.getTenant().getId(),
                modelId, false, true, false);
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importAndRTSBulkScoreWorkflow");
        if (modelSummary != null) {
            inputProperties.put(WorkflowContextConstants.Inputs.MODEL_DISPLAY_NAME, modelSummary.getDisplayName());
        }
        String dataCloudVersion = getLatestDataCloudVersion();
        boolean skipIdMatch = true;
        if (modelSummary != null) {
            skipIdMatch = !modelSummary.isMatch();
        }

        String modelType = modelSummary.getModelType();
        enableLeadEnrichment = enableLeadEnrichment & !modelType.equals(ModelType.PMML.getModelType());
        skipIdMatch = skipIdMatch && !enableLeadEnrichment;

        String sourceFileDisplayName = sourceFile.getDisplayName() != null ? sourceFile.getDisplayName() : "unnamed";
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);
        List<BucketMetadata> bucketMetadataList = bucketedScoreService.getUpToDateModelBucketMetadata(modelId);
        if (bucketMetadataList == null) {
            throw new LedpException(LedpCode.LEDP_18128, new String[] { modelId });
        }
        return new ImportAndRTSBulkScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort)//
                .reportNamePrefix(sourceFile.getName() + "_Report") //
                .modelId(modelId) //
                .modelType(modelType) //
                .sourceSchemaInterpretation(modelSummary.getSourceSchemaInterpretation()) //
                .inputTableName(sourceFile.getTableName()) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename(
                        "/" + StringUtils.substringBeforeLast(sourceFileDisplayName.replaceAll("[^A-Za-z0-9_]", "_"),
                                ".csv") + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .enableLeadEnrichment(enableLeadEnrichment) //
                .setScoreTestFile(true) //
                .enableDebug(enableDebug) //
                .matchDebugEnabled(!skipIdMatch && !ModelType.PMML.getModelType().equals(modelType)
                        && plsFeatureFlagService.isMatchDebugEnabled()) //
                .matchRequestSource(MatchRequestSource.ENRICHMENT) //
                .internalResourceHostPort(internalResourceHostPort) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("AccountMasterColumn") //
                .matchQueue(LedpQueueAssigner.getScoringQueueNameForSubmission()) //
                .matchColumnSelection(Predefined.ID, "1.0.0") //
                .dataCloudVersion(dataCloudVersion) //
                .excludeDataCloudAttrs(skipIdMatch) //
                .skipMatchingStep(ModelType.PMML.getModelType().equals(modelType)) //
                .matchClientDocument(matchClientDocument) //
                .bucketMetadata(bucketMetadataList) //
                .idColumnName(InterfaceName.InternalId.name()) //
                .workflowContainerMem(workflowMemMb) //
                .build();
    }
}
