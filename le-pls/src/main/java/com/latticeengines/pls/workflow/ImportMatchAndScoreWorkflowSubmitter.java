package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.eai.SourceType;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.MatchJoinType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndScoreWorkflowConfiguration;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.pls.service.SourceFileService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
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

    public ApplicationId submit(String modelId, String fileName, TransformationGroup transformationGroup) {
        SourceFile sourceFile = sourceFileService.findByName(fileName);

        if (sourceFile == null) {
            throw new LedpException(LedpCode.LEDP_18084, new String[] { fileName });
        }

        if (metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), sourceFile.getTableName()) == null) {
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

        log.info(String
                .format("Submitting testing data score workflow for modelId %s and tableToScore %s for customer %s and source %s",
                        modelId, sourceFile.getTableName(), MultiTenantContext.getCustomerSpace(),
                        sourceFile.getDisplayName()));
        return workflowJobService.submit(configuration);

    }

    public ImportMatchAndScoreWorkflowConfiguration generateConfiguration(String modelId, SourceFile sourceFile,
            String sourceDisplayName, TransformationGroup transformationGroup) {

        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "importMatchAndScoreWorkflow");

        ModelSummary summary = modelSummaryService.getModelSummaryEnrichedByDetails(modelId);

        ColumnSelection.Predefined selection = ColumnSelection.Predefined.getLegacyDefaultSelection();
        String selectionVersion = null;
        String dataCloudVersion = null;
        if (summary != null) {
            if (summary.getPredefinedSelection() != null) {
                selection = summary.getPredefinedSelection();
                if (StringUtils.isNotEmpty(summary.getPredefinedSelectionVersion())) {
                    selectionVersion = summary.getPredefinedSelectionVersion();
                }
            }
            dataCloudVersion = summary.getDataCloudVersion();
        }

        String sourceFileDisplayName = sourceFile.getDisplayName() != null ? sourceFile.getDisplayName() : "unnamed";

        return new ImportMatchAndScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .microServiceHostPort(microserviceHostPort) //
                .sourceFileName(sourceFile.getName()) //
                .sourceType(SourceType.FILE) //
                .internalResourceHostPort(internalResourceHostPort)//
                .reportNamePrefix(sourceFile.getName() + "_Report") //
                .modelId(modelId) //
                .inputTableName(sourceFile.getTableName()) //
                .matchClientDocument(matchClientDocument) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .matchColumnSelection(selection, selectionVersion) //
                .dataCloudVersion(dataCloudVersion) //
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename("/" + sourceFileDisplayName.replace(' ', '_') + "_scored_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .internalResourcePort(internalResourceHostPort) //
                .transformationGroup(transformationGroup) //
                .build();
    }
}
