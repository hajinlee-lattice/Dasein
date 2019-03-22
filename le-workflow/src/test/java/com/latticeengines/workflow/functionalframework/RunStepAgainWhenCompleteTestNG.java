package com.latticeengines.workflow.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;

public class RunStepAgainWhenCompleteTestNG extends WorkflowTestNGBase {

    @Autowired
    private FailableStep failableStep;

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Autowired
    private RunCompletedStepAgainWorkflow runCompletedStepAgainWorkflow;

    private WorkflowJob workflowJob1;
    private WorkflowJob workflowJob2;

    @AfterMethod(groups = "functional")
    public void cleanup() {
        super.cleanup(workflowJob1.getWorkflowId());
        super.cleanup(workflowJob2.getWorkflowId());
    }

    @Test(groups = "functional")
    public void testRunCompletedStepAgainWorkflow() throws Exception {
        failableStep.setFail(true);
        WorkflowConfiguration configuration = new WorkflowConfiguration();
        CustomerSpace customerSpace = CustomerSpace.parse(WORKFLOW_TENANT);
        configuration.setContainerConfiguration(runCompletedStepAgainWorkflow.name(), customerSpace,
                runCompletedStepAgainWorkflow.name());

        workflowService.registerJob(configuration, applicationContext);
        WorkflowExecutionId workflowId = workflowService.start(configuration);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT, 1000L).getStatus();
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(runAgainWhenCompleteStep.name()));
        assertTrue(stepNames.contains(successfulStep.name()));
        assertEquals(status, BatchStatus.FAILED);
        workflowJob1 = workflowJobEntityMgr.findByWorkflowId(workflowId.getId());

        failableStep.setFail(false);
        String appid = "appid_1";
        workflowJob2 = new WorkflowJob();
        workflowJob2.setTenant(MultiTenantContext.getTenant());
        workflowJob2.setApplicationId(appid);
        workflowJobEntityMgr.create(workflowJob2);

        WorkflowExecutionId restartedWorkflowId = workflowService.restart(workflowId, workflowJob2);
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT, 1000).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertTrue(restartedStepNames.contains(runAgainWhenCompleteStep.name()));
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
        assertEquals(workflowJobEntityMgr.findByApplicationId(appid).getWorkflowId().longValue(),
                restartedWorkflowId.getId());
        assertTrue(FailableStep.hasFlagInContext.get());
    }

}
