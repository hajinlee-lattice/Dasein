package com.latticeengines.cdl.workflow;

import static org.testng.Assert.assertEquals;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceflows-cdl-context.xml" })
public class CDLImportAndStageWorkflowDeploymentTestNG extends CDLWorkflowDeploymentTestNGBase {
    
    @Autowired
    private CDLImportWorkflow cdlImportWorkflow;

    @Autowired
    private CDLCreateStagingTablesWorkflow cdlCreateStagingTablesWorkflow;

    @Test(groups = "deployment")
    public void testWorkflow() throws Exception {
        CDLImportWorkflowConfiguration config = generateConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(cdlImportWorkflow.name(), config);

        System.out.println("Workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testWorkflow" })
    public void testCreateStagingTablesWorkflow() throws Exception {
        CDLCreateStagingTablesWorkflowConfiguration config = generateCreateStagingTablesConfiguration();

        WorkflowExecutionId workflowId = workflowService.start(cdlCreateStagingTablesWorkflow.name(), config);

        System.out.println("Create staging tables workflow id = " + workflowId.getId());
        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    private CDLCreateStagingTablesWorkflowConfiguration generateCreateStagingTablesConfiguration() {
        CDLCreateStagingTablesWorkflowConfiguration.Builder builder = new CDLCreateStagingTablesWorkflowConfiguration.Builder();
        return builder.internalResourceHostPort(plsUrl) //
                .microServiceHostPort(microServiceHostPort) //
                .customer(customer) //
                .sourceFile("Account", sourceFileMap.get("Account")) //
                .sourceFile("Transaction", sourceFileMap.get("Transaction")) //
                .sourceFile("Product", sourceFileMap.get("Product")) //
                .build();
    }

    private CDLImportWorkflowConfiguration generateConfiguration() {
        CDLImportWorkflowConfiguration.Builder builder = new CDLImportWorkflowConfiguration.Builder();
        return builder.microServiceHostPort(microServiceHostPort) //
                .internalResourceHostPort(plsUrl) //
                .customer(customer) //
                .sourceFile("Account", sourceFileMap.get("Account")) //
                .sourceFile("Transaction", sourceFileMap.get("Transaction")) //
                .sourceFile("Product", sourceFileMap.get("Product")) //
                .build();
    }
}
