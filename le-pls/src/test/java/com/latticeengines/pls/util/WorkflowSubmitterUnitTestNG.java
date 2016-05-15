package com.latticeengines.pls.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.pls.service.WorkflowJobService;
import com.latticeengines.pls.workflow.FitWorkflowSubmitter;

public class WorkflowSubmitterUnitTestNG {

    private FitWorkflowSubmitter fitWorkflowSubmitter = new FitWorkflowSubmitter();

    @Test(groups = "unit")
    public void submitFitWorkflow() {
        TargetMarket targetMarket = mock(TargetMarket.class);
        WorkflowJobService workflowJobService = mock(WorkflowJobService.class);
        when(targetMarket.getApplicationId()).thenReturn("application_xyz_123");

        when(workflowJobService.getJobStatusFromApplicationId("application_xyz_123")).thenReturn(JobStatus.RUNNING);

        ReflectionTestUtils.setField(fitWorkflowSubmitter, "workflowJobService", workflowJobService);

        boolean exception = false;
        try {
            fitWorkflowSubmitter.submitWorkflowForTargetMarketAndWorkflowName(targetMarket, "fitModelWorkflow");
        } catch (Exception e) {
            exception = true;
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18076);
        }
        assertTrue(exception);
    }

}
