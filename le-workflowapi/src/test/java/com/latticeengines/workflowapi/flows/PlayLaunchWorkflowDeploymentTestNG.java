package com.latticeengines.workflowapi.flows;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowExecutionId;
import com.latticeengines.workflowapi.functionalframework.WorkflowApiDeploymentTestNGBase;

public class PlayLaunchWorkflowDeploymentTestNG extends WorkflowApiDeploymentTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

    WorkflowExecutionId workflowId = null;

    @Test(groups = "workflow")
    public void testWorkflow() throws Exception {
        PlayLaunchWorkflowConfiguration configuration = generatePlayLaunchWorkflowConfiguration();
        workflowService.registerJob(configuration.getWorkflowName(), applicationContext);
        workflowId = workflowService.start(configuration);
    }

    @Test(groups = "workflow", dependsOnMethods = { "testWorkflow" }, expectedExceptions = AssertionError.class)
    public void testWorkflowStatus() throws Exception {
        waitForCompletion(workflowId);
    }

    private PlayLaunchWorkflowConfiguration generatePlayLaunchWorkflowConfiguration() {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");
        return new PlayLaunchWorkflowConfiguration.Builder() //
                .customer(mainTestCustomerSpace) //
                .workflow("playLaunchWorkflow") //
                .inputProperties(inputProperties) //
                .playName("DUMMY_PLAY_NAME") //
                .playLaunchId("DUMMY_PLAY_LAUNCH_ID") //
                .build();
    }
}
