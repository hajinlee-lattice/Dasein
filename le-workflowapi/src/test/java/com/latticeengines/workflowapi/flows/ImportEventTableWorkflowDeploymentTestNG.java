package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.SchemaInterpretation;
import com.latticeengines.domain.exposed.workflow.SourceFile;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.leadprioritization.workflow.ImportEventTableWorkflow;
import com.latticeengines.leadprioritization.workflow.ImportEventTableWorkflowConfiguration;

public class ImportEventTableWorkflowDeploymentTestNG extends ImportEventTableWorkflowTestNGBase {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(ImportEventTableWorkflowDeploymentTestNG.class);

    private static final String RESOURCE_BASE = "com/latticeengines/workflowapi/flows/leadprioritization/csvfiles";

    @Autowired
    private ImportEventTableWorkflow importEventTableWorkflow;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupForImportWorkflow();
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflowAccount() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Account.csv", SchemaInterpretation.SalesforceAccount);
        run(sourceFile);
    }

    @Test(groups = "deployment", enabled = false)
    public void testWorkflowLead() throws Exception {
        SourceFile sourceFile = uploadFile(RESOURCE_BASE + "/Lead.csv", SchemaInterpretation.SalesforceLead);
        run(sourceFile);
    }

    private void run(SourceFile sourceFile) throws Exception {
        ImportEventTableWorkflowConfiguration workflowConfig = generateWorkflowConfig(sourceFile);
        WorkflowExecutionId workflowId = workflowService.start(importEventTableWorkflow.name(), workflowConfig);

        log.info("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }
}
