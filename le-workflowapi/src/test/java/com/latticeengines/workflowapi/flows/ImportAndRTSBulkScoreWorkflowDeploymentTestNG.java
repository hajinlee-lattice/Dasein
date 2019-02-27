package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertNotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.SchemaInterpretation;
import com.latticeengines.domain.exposed.pls.SourceFile;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.ImportAndRTSBulkScoreWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.pls.workflow.ImportAndRTSBulkScoreWorkflowSubmitter;

public class ImportAndRTSBulkScoreWorkflowDeploymentTestNG extends ScoreWorkflowDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ImportAndRTSBulkScoreWorkflowDeploymentTestNG.class);

    @Autowired
    private ImportAndRTSBulkScoreWorkflowSubmitter importAndRTSBulkScoreWorkflowSubmitter;

    private SourceFile sourceFile;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        setupTestEnvironment(LatticeProduct.LPA3);
        sourceFile = uploadFile(RESOURCE_BASE + "/csvfiles/Account.csv", SchemaInterpretation.SalesforceAccount);
        setupModels();
    }

    @Override
    @Test(groups = "workflow", enabled = true)
    public void scoreAccount() throws Exception {
        ModelSummary summary = locateModelSummary("testWorkflowAccount", mainTestCustomerSpace);
        assertNotNull(summary);
        createModelSummary(summary, mainTestTenant.getId());
        log.info("summry id is " + summary.getId());
        score(summary.getId(), sourceFile.getName());
    }

    private void score(String modelId, String tableToScore) throws Exception {
        ImportAndRTSBulkScoreWorkflowConfiguration workflowConfig = importAndRTSBulkScoreWorkflowSubmitter
                .generateConfiguration(modelId, sourceFile, "Testing RTS Bulk Score Data", false, false);
        System.out.println(workflowConfig);
        workflowService.registerJob(workflowConfig, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(workflowConfig);
        waitForCompletion(workflowId);
    }
}
