package com.latticeengines.workflowapi.flows;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.springframework.batch.core.BatchStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.rest.URLUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.domain.exposed.workflow.WorkflowStatus;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiFunctionalTestNGBase;
import com.latticeengines.workflowapi.steps.prospectdiscovery.BaseFitModelStepConfiguration;

public class FitModelWorkflowTestNG extends WorkflowApiFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(FitModelWorkflowTestNG.class);
    private static final CustomerSpace DEMO_CUSTOMERSPACE = CustomerSpace.parse("DemoContract.DemoTenant.Production");
    private static final String microServiceHostPort = "http://localhost:8080";
    private static final String modelingServiceHdfsBaseDir = "/user/s-analytics/customers/";

    @Autowired
    private FitModelWorkflow fitModelWorkflow;

    @Override
    protected boolean enableJobRepositoryCleanupBeforeTest() {
        return false;
    }

    @Test(groups = "functional", enabled = true)
    public void testWorkflow() throws Exception {
        BaseFitModelStepConfiguration baseFitModelStepConfiguration = new BaseFitModelStepConfiguration();
        baseFitModelStepConfiguration.setCustomerSpace(DEMO_CUSTOMERSPACE.toString());
        baseFitModelStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
        baseFitModelStepConfiguration.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);

        FitModelWorkflowConfiguration workflowConfig = new FitModelWorkflowConfiguration.Builder()
                .setBaseFitModelStepConfiguration(baseFitModelStepConfiguration).build();

        WorkflowExecutionId workflowId = workflowService.start(fitModelWorkflow.name(), workflowConfig);

        BatchStatus status = workflowService.waitForCompletion(workflowId, WORKFLOW_WAIT_TIME_IN_MILLIS).getStatus();
        assertEquals(status, BatchStatus.COMPLETED);
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflowInContainer() throws Exception {
        BaseFitModelStepConfiguration baseFitModelStepConfiguration = new BaseFitModelStepConfiguration();
        baseFitModelStepConfiguration.setCustomerSpace(DEMO_CUSTOMERSPACE.toString());
        baseFitModelStepConfiguration.setMicroServiceHostPort(microServiceHostPort);
        baseFitModelStepConfiguration.setModelingServiceHdfsBaseDir(modelingServiceHdfsBaseDir);

        FitModelWorkflowConfiguration workflowConfig = new FitModelWorkflowConfiguration.Builder()
                .setBaseFitModelStepConfiguration(baseFitModelStepConfiguration).build();
        workflowConfig.setContainerConfiguration(fitModelWorkflow.name(), DEMO_CUSTOMERSPACE,
                "FitModelWorkflowTest_submitWorkflow");

        AppSubmission submission = submitWorkflow(workflowConfig);
        assertNotNull(submission);
        assertNotEquals(submission.getApplicationIds().size(), 0);
        String appId = submission.getApplicationIds().get(0);
        assertNotNull(appId);
        FinalApplicationStatus status = platformTestBase.waitForStatus(appId, WORKFLOW_WAIT_TIME_IN_MILLIS, FinalApplicationStatus.SUCCEEDED);
        assertEquals(status, FinalApplicationStatus.SUCCEEDED);

        String url = String.format("%s/workflowapi/workflows/yarnapps/%s", URLUtils.getRestAPIHostPort(hostPort), appId);
        String workflowId = restTemplate.getForObject(url, String.class);

        url = String.format("%s/workflowapi/workflows/%s", URLUtils.getRestAPIHostPort(hostPort), workflowId);
        WorkflowStatus workflowStatus = restTemplate.getForObject(url, WorkflowStatus.class);
        assertEquals(workflowStatus.getStatus(), BatchStatus.COMPLETED);
    }
}
