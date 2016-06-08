package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.ImportAndRTSBulkScoreWorkflow;
import com.latticeengines.leadprioritization.workflow.ImportAndRTSBulkScoreWorkflowConfiguration;
import com.latticeengines.pls.workflow.ImportAndRTSBulkScoreWorkflowSubmitter;

public class ImportAndRTSBulkScoreWorkflowDeloymentTestNG extends ScoreWorkflowDeploymentTestNG {

    @Autowired
    private ImportAndRTSBulkScoreWorkflow importAndRTSBulkScoreWorkflow;

    @Autowired
    private ImportAndRTSBulkScoreWorkflowSubmitter importAndRTABulkScoreWorkflowSubmitter;

    private SourceFile sourceFile;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
        sourceFile = uploadFile(RESOURCE_BASE + "/csvfiles/Account.csv", SchemaInterpretation.SalesforceAccount);
        setupModels();
    }

    @Override
    @Test(groups = "deployment", enabled = false)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        score(summary.getId(), sourceFile.getName());
    }

    private void score(String modelId, String tableToScore) throws Exception {
        ImportAndRTSBulkScoreWorkflowConfiguration configuration = importAndRTABulkScoreWorkflowSubmitter
                .generateConfiguration(modelId, sourceFile, "Testing RTS Bulk Score Data");
        WorkflowExecutionId workflowId = workflowService.start(importAndRTSBulkScoreWorkflow.name(), configuration);

        waitForCompletion(workflowId);
    }
}
