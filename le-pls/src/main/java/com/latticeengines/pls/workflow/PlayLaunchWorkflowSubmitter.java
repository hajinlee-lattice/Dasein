package com.latticeengines.pls.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.serviceflows.cdl.PlayLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("playLaunchWorkflowSubmitter")
public class PlayLaunchWorkflowSubmitter extends WorkflowSubmitter {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowSubmitter.class);

    public ApplicationId submit(PlayLaunch playLaunch) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "playLaunchWorkflow");

        PlayLaunchWorkflowConfiguration configuration = new PlayLaunchWorkflowConfiguration.Builder()
                .customer(getCustomerSpace()) //
                .workflow("playLaunchWorkflow") //
                .inputProperties(inputProperties) //
                .playName(playLaunch.getPlay().getName()) //
                .playLaunchId(playLaunch.getLaunchId()) //
                .build();
        return workflowJobService.submit(configuration);
    }
}
