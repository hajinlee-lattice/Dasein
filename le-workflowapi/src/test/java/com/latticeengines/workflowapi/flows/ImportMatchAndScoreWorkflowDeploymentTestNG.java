package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.transform.TransformationGroup;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndScoreWorkflow;
import com.latticeengines.leadprioritization.workflow.ImportMatchAndScoreWorkflowConfiguration;
import com.latticeengines.pls.workflow.ImportMatchAndScoreWorkflowSubmitter;

public class ImportMatchAndScoreWorkflowDeploymentTestNG extends ScoreWorkflowDeploymentTestNG {

    @Autowired
    private ImportMatchAndScoreWorkflow importMatchAndScoreWorkflow;

    @Autowired
    private ImportMatchAndScoreWorkflowSubmitter importMatchAndScoreWorkflowSubmitter;

    private SourceFile sourceFile;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForWorkflow();
        sourceFile = uploadFile(RESOURCE_BASE + "/csvfiles/Account.csv", SchemaInterpretation.SalesforceAccount);
        setupModels();
    }

    @Override
    @Test(groups = "deployment", enabled = true)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", DEMO_CUSTOMERSPACE);
        assertNotNull(summary);
        score(summary.getId(), sourceFile.getName(), TransformationGroup.STANDARD);
    }

    private void score(String modelId, String tableToScore, TransformationGroup transformationGroup) throws Exception {
        ImportMatchAndScoreWorkflowConfiguration configuration = importMatchAndScoreWorkflowSubmitter
                .generateConfiguration(modelId, sourceFile, "Testing Data", transformationGroup);
        WorkflowExecutionId workflowId = workflowService.start(importMatchAndScoreWorkflow.name(), configuration);

        waitForCompletion(workflowId);
    }
}
