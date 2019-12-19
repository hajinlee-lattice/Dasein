package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("campaignDeltaCalculationWorkflowListener")

public class CampaignDeltaCalculationWorkflowListener extends LEJobListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignDeltaCalculationWorkflowListener.class);

    @Inject
    private PlayProxy playProxy;

    @Inject

    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        try {
            if (jobExecution.getStatus().isUnsuccessful()) {
                WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
                String customerSpace = job.getTenant().getId();
                String playId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_NAME);
                String channelId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_CHANNEL_ID);
                String launchId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID);
                log.warn(String.format(
                        "Campaign Delta Calculation Workflow failed for Campaign %s, Channel %s for customer %s",
                        playId, channelId, customerSpace));
                if (StringUtils.isNotBlank(launchId)) {
                    playProxy.updatePlayLaunch(customerSpace, playId, launchId, LaunchState.Failed);
                    log.warn(String.format("Updated the launch state to failed for Launch %s ", launchId));
                } else {
                    PlayLaunch launch = playProxy.createNewLaunchByPlayChannelAndState(customerSpace, playId, channelId,
                            LaunchState.Failed, null, null, null, null, null, true);
                    launchId = launch.getId();
                    log.warn(String.format("Created a new launch (%s) with failed state to log the failure event",
                            launchId));
                }
                PlayLaunchChannel channel = playProxy.getChannelById(customerSpace, playId, channelId);
                if (channel.getIsAlwaysOn()) {
                    playProxy.setNextScheduledTimeForChannel(customerSpace, playId, channelId);
                }
            }
        } catch (Exception e) {
            log.error("Failed to execute Listener for CampaignDeltaCalculationWorkflow successfully", e.getMessage());
        }
    }
}
