package com.latticeengines.cdl.workflow.listeners;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("campaignDeltaCalculationWorkflowListener")

public class CampaignDeltaCalculationWorkflowListener extends LEJobListener {
    private static final Logger log = LoggerFactory.getLogger(CampaignDeltaCalculationWorkflowListener.class);

    private String customerSpace;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private EmailProxy emailProxy;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    private static final String PREVIOUS_ACCOUNTS_UNIVERSE = "PREVIOUS_ACCOUNTS_UNIVERSE";
    private static final String PREVIOUS_CONTACTS_UNIVERSE = "PREVIOUS_CONTACTS_UNIVERSE";

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        try {
            if (jobExecution.getStatus().isUnsuccessful()) {
                WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
                customerSpace = job.getTenant().getId();
                String playId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_NAME);
                String channelId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_CHANNEL_ID);
                String launchId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID);
                log.warn(String.format(
                        "Campaign Delta Calculation Workflow failed for Campaign %s, Channel %s for customer %s",
                        playId, channelId, customerSpace));
                if (StringUtils.isNotBlank(launchId)) {
                    log.warn(String.format("Updated the launch state to failed for Launch %s ", launchId));
                    updateFailedPlayLaunch(playId, launchId);
                } else {
                    PlayLaunch launch = new PlayLaunch();
                    launch.setLaunchState(LaunchState.Failed);
                    launch.setParentDeltaWorkflowId(job.getPid());
                    launch.setLaunchType(playProxy.getChannelById(customerSpace, playId, channelId).getLaunchType());
                    launch = playProxy.createNewLaunchByPlayAndChannel(customerSpace, playId, channelId, true, launch);
                    launchId = launch.getId();
                    log.warn(String.format("Created a new launch (%s) with failed state to log the failure event",
                            launchId));
                }

                PlayLaunchChannel channel = playProxy.getChannelById(customerSpace, playId, channelId);
                channel.setPreviousLaunchedAccountUniverseTable(
                        getStringValueFromContext(jobExecution, PREVIOUS_ACCOUNTS_UNIVERSE));
                channel.setPreviousLaunchedContactUniverseTable(
                        getStringValueFromContext(jobExecution, PREVIOUS_CONTACTS_UNIVERSE));
                log.warn(String.format(
                        "Reverting the Launch universe to the launch universe prior to the current workflow AccountUniverseTable:%s ContactUniverseTable: %s ",
                        channel.getCurrentLaunchedAccountUniverseTable(),
                        channel.getCurrentLaunchedContactUniverseTable()));
                playProxy.recoverPlayLaunchChannelLaunchUniverse(customerSpace, playId, channelId, channel);
                Play play = playProxy.getPlay(customerSpace, playId);
                PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace, playId, launchId);
                playLaunch.setPlay(play);
                try {
                    emailProxy.sendPlayLaunchErrorEmail(customerSpace,
                            channel.getUpdatedBy(), playLaunch);
                } catch (Exception e) {
                    log.error("Can not send play launch failed email: " + e.getMessage());
                }
            }
        } catch (Exception e) {
            log.error("Failed to execute Listener for CampaignDeltaCalculationWorkflow successfully", e);
        }
    }

    private void updateFailedPlayLaunch(String playName, String playLaunchId) {
        PlayLaunch launch = playProxy.getPlayLaunch(customerSpace, playName, playLaunchId);
        long accountsAdded = getCount(launch.getAccountsAdded());
        long accountsDeleted = getCount(launch.getAccountsDeleted());
        long accountsLaunched = getCount(launch.getAccountsLaunched());
        long contactsAdded = getCount(launch.getContactsAdded());
        long contactsDeleted = getCount(launch.getContactsDeleted());
        long contactsLaunched = getCount(launch.getContactsLaunched());

        launch.setLaunchState(LaunchState.Failed);
        launch.setAccountsAdded(0L);
        launch.setAccountsDeleted(0L);
        launch.setAccountsDuplicated(0L);
        // equal to previous accumulative launched
        launch.setAccountsLaunched(accountsLaunched - accountsAdded + accountsDeleted);
        // equal to incremental launched
        launch.setAccountsErrored(accountsAdded + accountsDeleted);
        launch.setContactsAdded(0L);
        launch.setContactsDeleted(0L);
        launch.setContactsDuplicated(0L);
        // equal to previous accumulative launched
        launch.setContactsLaunched(contactsLaunched - contactsAdded + contactsDeleted);
        // equal to incremental launched
        launch.setContactsErrored(contactsAdded + contactsDeleted);

        PlayLaunch playLaunch = playProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, launch);
        log.info("After updat, PlayLauunch = " + JsonUtils.serialize(playLaunch));
    }

    private long getCount(Long object) {
        return object != null ? object.longValue() : 0L;
    }
}
