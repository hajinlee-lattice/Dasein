package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.workflowThrottling.FakeApplicationId;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.Job;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.workflowapi.WorkflowProxy;

@Component("campaignDeltaCalculationWorkflowSubmitter")
public class CampaignDeltaCalculationWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CampaignDeltaCalculationWorkflowSubmitter.class);

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private WorkflowProxy workflowProxy;

    public Long submit(String customerSpace, String playId, String channelId) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "campaignDeltaCalculationWorkflow");
        DataCollection.Version version = dataCollectionService.getActiveVersion(getCustomerSpace().toString());
        CampaignDeltaCalculationWorkflowConfiguration configuration = new CampaignDeltaCalculationWorkflowConfiguration.Builder()
                .workflow("campaignDeltaCalculationWorkflow") //
                .dataCollectionVersion(version) //
                .customer(StringUtils.isEmpty(customerSpace) ? getCustomerSpace() : CustomerSpace.parse(customerSpace))
                .inputProperties(inputProperties) //
                .playId(playId) //
                .channelId(channelId) //
                .executionId(UUID.randomUUID().toString()) //
                .build();
        ApplicationId appId = workflowJobService.submit(configuration);

        if (FakeApplicationId.isFakeApplicationId(appId.toString())) {
            return FakeApplicationId.toWorkflowJobPid(appId.toString());
        } else {
            Job job = workflowProxy.getWorkflowJobFromApplicationId(appId.toString(), customerSpace);
            return job.getPid();
        }

    }
}
