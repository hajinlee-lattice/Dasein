package com.latticeengines.workflowapi.flows;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

public class PlayLaunchWorkflowDeploymentTestNG extends PlayLaunchWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(PlayLaunchWorkflowDeploymentTestNG.class);

    WorkflowExecutionId workflowId = null;

    @BeforeClass(groups = { "deployment" })
    public void setup() throws Exception {
        setupForPlayLaunch();
    }

    @AfterClass(groups = "deployment")
    public void cleanup() throws Exception {
        cleanUpAfterPlayLaunch();
    }

    @Test(groups = "deployment", enabled = true)
    public void testWorkflow() throws Exception {
        PlayLaunchWorkflowConfiguration configuration = generatePlayLaunchWorkflowConfiguration();
        workflowService.registerJob(configuration.getWorkflowName(), applicationContext);
        workflowId = workflowService.start(configuration.getWorkflowName(), configuration);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testWorkflow" }, expectedExceptions = AssertionError.class)
    public void testWorkflowStatus() throws Exception {
        waitForCompletion(workflowId);
    }
}
