package com.latticeengines.pls.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.springframework.batch.core.BatchStatus;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.pls.workflow.FitWorkflowSubmitter;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

public class WorkflowSubmitterUnitTestNG {
    
    private FitWorkflowSubmitter fitWorkflowSubmitter = new FitWorkflowSubmitter();

    @Test(groups = "unit")
    public void submitFitWorkflow() {
        TargetMarket targetMarket = mock(TargetMarket.class);
        WorkflowProxy workflowProxy = mock(WorkflowProxy.class);
        WorkflowStatus workflowStatus = mock(WorkflowStatus.class);
        when(targetMarket.getApplicationId()).thenReturn("application_xyz_123");
        
        when(workflowProxy.getWorkflowStatusFromApplicationId("application_xyz_123")).thenReturn(workflowStatus);
        when(workflowStatus.getStatus()).thenReturn(BatchStatus.STARTED);
        
        ReflectionTestUtils.setField(fitWorkflowSubmitter, "workflowProxy", workflowProxy);
        
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
