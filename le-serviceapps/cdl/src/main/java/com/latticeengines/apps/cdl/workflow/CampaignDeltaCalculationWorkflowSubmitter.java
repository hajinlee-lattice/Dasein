package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

@Component("campaignDeltaCalculationWorkflowSubmitter")
public class CampaignDeltaCalculationWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CampaignDeltaCalculationWorkflowSubmitter.class);

    public ApplicationId submit(String playId, String channelId) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "campaignDeltaCalculationWorkflow");

        CampaignDeltaCalculationWorkflowConfiguration configuration = new CampaignDeltaCalculationWorkflowConfiguration.Builder()
                .workflow("campaignDeltaCalculationWorkflow").customer(getCustomerSpace())
                .inputProperties(inputProperties).playId(playId).channelId(channelId).build();
        return workflowJobService.submit(configuration);
    }
}
