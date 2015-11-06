package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;

import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.workflow.WorkflowId;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.steps.prospectdiscovery.BaseFitModelStepConfiguration;

public class FitModelWorkflowTestNG extends WorkflowApiFunctionalTestNGBase {

    private static final String DEMO_CUSTOMERSPACE = "DemoContract.DemoTenant.Production";
    private static final String microServiceHostPort = "http://localhost:8080";
    private static final String modelingServiceHdfsBaseDir = "/user/s-analytics/customers/";

    @Autowired
    private FitModelWorkflow fitModelWorkflow;

    @Test(groups = "functional", enabled = true)
    public void testWorkflow() throws Exception {
        BaseFitModelStepConfiguration baseFitModelStepConfiguration = new BaseFitModelStepConfiguration();
        baseFitModelStepConfiguration.setCustomerSpace(DEMO_CUSTOMERSPACE);
        baseFitModelStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
        baseFitModelStepConfiguration.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);

        FitModelWorkflowConfiguration workflowConfig = new FitModelWorkflowConfiguration.Builder()
                .setBaseFitModelStepConfiguration(baseFitModelStepConfiguration).build();
//        workflowConfig.setContainerConfiguration(fitModelWorkflow.name(), DEMO_CUSTOMERSPACE,
//                "FitModelWorkflowTest_submitWorkflow");

        WorkflowId workflowId = workflowService.start(fitModelWorkflow.name(), workflowConfig);
        BatchStatus status = workflowService.waitForCompletion(workflowId, MAX_MILLIS_TO_WAIT).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

}
