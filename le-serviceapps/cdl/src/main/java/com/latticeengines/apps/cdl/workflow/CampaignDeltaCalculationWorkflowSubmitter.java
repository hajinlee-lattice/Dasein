package com.latticeengines.apps.cdl.workflow;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.core.workflow.WorkflowSubmitter;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.CampaignDeltaCalculationWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;

@Component("campaignDeltaCalculationWorkflowSubmitter")
public class CampaignDeltaCalculationWorkflowSubmitter extends WorkflowSubmitter {

    private static final Logger log = LoggerFactory.getLogger(CampaignDeltaCalculationWorkflowSubmitter.class);

    @Inject
    private DataCollectionProxy dataCollectionProxy;

    public ApplicationId submit(String customerSpace, String playId, String channelId) {
        Map<String, String> inputProperties = new HashMap<>();
        inputProperties.put(WorkflowContextConstants.Inputs.JOB_TYPE, "campaignDeltaCalculationWorkflow");
        DataCollection.Version version = dataCollectionProxy.getActiveVersion(getCustomerSpace().toString());
        log.info("In Submitter: " + customerSpace);
        CampaignDeltaCalculationWorkflowConfiguration configuration = new CampaignDeltaCalculationWorkflowConfiguration.Builder()
                .workflow("campaignDeltaCalculationWorkflow") //
                .dataCollectionVersion(version) //
                .customer(StringUtils.isEmpty(customerSpace) ? getCustomerSpace() : CustomerSpace.parse(customerSpace))
                .inputProperties(inputProperties) //
                .playId(playId) //
                .channelId(channelId) //
                .build();
        return workflowJobService.submit(configuration);
    }
}
