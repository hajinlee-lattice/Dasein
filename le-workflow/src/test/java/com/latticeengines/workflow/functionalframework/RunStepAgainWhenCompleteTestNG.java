package com.latticeengines.workflow.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.security.exposed.util.MultiTenantContext;

public class RunStepAgainWhenCompleteTestNG extends WorkflowTestNGBase {

    @Autowired
    private FailableStep failableStep;

    @Autowired
    private SuccessfulStep successfulStep;

    @Autowired
    private RunAgainWhenCompleteStep runAgainWhenCompleteStep;

    @Autowired
    private RunCompletedStepAgainWorkflow runCompletedStepAgainWorkflow;

    @Test(groups = "functional", enabled = true)
    public void testRunCompletedStepAgainWorkflow() throws Exception {
        failableStep.setFail(true);
        WorkflowConfiguration configuration = new WorkflowConfiguration();
        CustomerSpace customerSpace = CustomerSpace.parse(WORKFLOW_TENANT);
        configuration.setContainerConfiguration("completedStepAgainWorkflow", customerSpace,
                "CompletedStepAgainWorkflow");
        WorkflowExecutionId workflowId = workflowService.start(runCompletedStepAgainWorkflow.name(), configuration);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> stepNames = workflowService.getStepNames(workflowId);
        assertTrue(stepNames.contains(runAgainWhenCompleteStep.name()));
        assertTrue(stepNames.contains(successfulStep.name()));
        assertEquals(status, BatchStatus.FAILED);

        failableStep.setFail(false);
        String appid = "appid_1";
        WorkflowJob workflowJob = new WorkflowJob();
        workflowJob.setTenant(MultiTenantContext.getTenant());
        workflowJob.setApplicationId(appid);
        workflowJobEntityMgr.create(workflowJob);
        WorkflowExecutionId restartedWorkflowId = workflowService.restart(workflowId, workflowJob);
        status = workflowService.waitForCompletion(restartedWorkflowId, MAX_MILLIS_TO_WAIT).getStatus();
        List<String> restartedStepNames = workflowService.getStepNames(restartedWorkflowId);
        assertTrue(restartedStepNames.contains(runAgainWhenCompleteStep.name()));
        assertFalse(restartedStepNames.contains(successfulStep.name()));
        assertEquals(status, BatchStatus.COMPLETED);
        assertEquals(workflowJobEntityMgr.findByApplicationId(appid).getWorkflowId().longValue(),
                restartedWorkflowId.getId());
    }

}
