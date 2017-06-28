package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.leadprioritization.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("playLaunchWorkflowSubmitter")
public class PlayLaunchWorkflowSubmitter extends WorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = Logger.getLogger(PlayLaunchWorkflowSubmitter.class);

    public ApplicationId submit(String launchId) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");
        inputProperties.put("playLaunchId", launchId);

        PlayLaunchWorkflowConfiguration configuration = new PlayLaunchWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow("playLaunchWorkflow") //
                .inputProperties(inputProperties) //
                .build();
        return workflowJobService.submit(configuration);
    }
}
