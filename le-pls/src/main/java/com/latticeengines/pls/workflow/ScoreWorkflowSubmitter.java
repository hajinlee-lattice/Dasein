package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.eai.ExportFormat;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.MatchClientDocument;
import com.latticeengines.domain.exposed.propdata.MatchCommandType;
import com.latticeengines.domain.exposed.propdata.MatchJoinType;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.leadprioritization.workflow.ScoreWorkflowConfiguration;
import com.latticeengines.pls.service.ModelSummaryService;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.propdata.MatchCommandProxy;
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

    public ApplicationId submit(String modelId, String tableToScore, String sourceDisplayName, TransformationGroup transformationGroup) {
        log.info(String.format(
                "Submitting score workflow for modelId %s and tableToScore %s for customer %s and source %s", modelId,
                tableToScore, MultiTenantContext.getCustomerSpace(), sourceDisplayName));
        ScoreWorkflowConfiguration configuration = generateConfiguration(modelId, tableToScore, sourceDisplayName, transformationGroup);

        if (metadataProxy.getTable(MultiTenantContext.getCustomerSpace().toString(), tableToScore) == null) {
            throw new LedpException(LedpCode.LEDP_18098, new String[] { tableToScore });
        }

        if (!modelSummaryService.modelIdinTenant(modelId, MultiTenantContext.getCustomerSpace().toString())) {
            throw new LedpException(LedpCode.LEDP_18007, new String[] { modelId });
        }

        return workflowJobService.submit(configuration);
    }

    public ScoreWorkflowConfiguration generateConfiguration(String modelId, String tableToScore,
            String sourceDisplayName, TransformationGroup transformationGroup) {
        MatchClientDocument matchClientDocument = matchCommandProxy.getBestMatchClient(3000);

        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.SOURCE_DISPLAY_NAME, sourceDisplayName);
        inputProperties.put(WorkflowContextConstants.Inputs.MODEL_ID, modelId);
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "scoreWorkflow");

        return new ScoreWorkflowConfiguration.Builder() //
                .customer(MultiTenantContext.getCustomerSpace()) //
                .matchClientDocument(matchClientDocument) //
                .microServiceHostPort(microserviceHostPort) //
                .internalResourcePort(internalResourceHostPort) //
                .modelId(modelId) //
                .inputTableName(tableToScore) //
                .matchJoinType(MatchJoinType.OUTER_JOIN) //
                .matchType(MatchCommandType.MATCH_WITH_UNIVERSE) //
                .matchDestTables("DerivedColumnsCache") //
                .columnSelection(ColumnSelection.Predefined.DerivedColumns, "1.0") // the version here should be read from model
                .outputFileFormat(ExportFormat.CSV) //
                .outputFilename("/Export_" + DateTime.now().getMillis()) //
                .inputProperties(inputProperties) //
                .transformationGroup(transformationGroup) //
                .build();
    }
}
