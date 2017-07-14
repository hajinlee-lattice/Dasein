package com.latticeengines.workflowapi.flows;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;

public class PlayLaunchWorkflowDeploymentTestNG extends PlayLaunchWorkflowTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

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
        workflowId = workflowService.start(configuration);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = {
            "testWorkflow" }, expectedExceptions = AssertionError.class)
    public void testWorkflowStatus() throws Exception {
        waitForCompletion(workflowId);
    }
}
