package com.latticeengines.cdl.workflow.listeners;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.serviceflows.cdl.DeltaCampaignLaunchWorkflowConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;
import com.latticeengines.domain.exposed.workflow.WorkflowJob;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.pls.EmailProxy;
import com.latticeengines.workflow.exposed.entitymanager.WorkflowJobEntityMgr;
import com.latticeengines.workflow.listener.LEJobListener;

@Component("deltaCampaignLaunchWorkflowListener")
public class DeltaCampaignLaunchWorkflowListener extends LEJobListener {

    private static final Logger log = LoggerFactory.getLogger(DeltaCampaignLaunchWorkflowListener.class);

    @Inject
    private PlayProxy playProxy;

    @Inject
    private EmailProxy emailProxy;

    @Inject
    private Configuration yarnConfiguration;

    private String customerSpace;

    @Inject
    private WorkflowJobEntityMgr workflowJobEntityMgr;

    @Override
    public void beforeJobExecution(JobExecution jobExecution) {

    }

    @Override
    public void afterJobExecution(JobExecution jobExecution) {
        WorkflowJob job = workflowJobEntityMgr.findByWorkflowId(jobExecution.getId());
        customerSpace = job.getTenant().getId();
        String playName = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_NAME);
        String playLaunchId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_ID);
        String channelId = job.getInputContextValue(WorkflowContextConstants.Inputs.PLAY_LAUNCH_CHANNEL_ID);

        try {
            if (jobExecution.getStatus().isUnsuccessful()) {
                log.warn(String.format("DeltaCampaignLaunch failed. Update launch %s of Campaign %s for customer %s",
                        playLaunchId, playName, customerSpace));
                recoverLaunchUniverses(customerSpace, playName, channelId);
                updateFailedPlayLaunch(playName, playLaunchId);
                Play play = playProxy.getPlay(customerSpace, playName);
                PlayLaunch playLaunch = playProxy.getPlayLaunch(customerSpace, playName, playLaunchId);
                playLaunch.setPlay(play);
                PlayLaunchChannel channel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace, playName,
                        playLaunch.getId());
                try {
                    emailProxy.sendPlayLaunchErrorEmail(customerSpace,
                            channel.getUpdatedBy(), playLaunch);
                } catch (Exception e) {
                    log.error("Can not send play launch failed email: " + e.getMessage());
                }
            } else {
                log.info(String.format(
                        "DeltaCampaignLaunch is successful. Update launch %s of Campaign %s for customer %s",
                        playLaunchId, playName, customerSpace));
                PlayLaunchChannel channel = playProxy.getPlayLaunchChannelFromPlayLaunch(customerSpace, playName,
                        playLaunchId);
                playProxy.updatePlayLaunch(customerSpace, playName, playLaunchId, LaunchState.Launched);
                playProxy.publishTalkingPoints(customerSpace, playName, channel.getUpdatedBy());
            }
        } finally {
            cleanupIntermediateFiles(jobExecution);
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
        log.info("After update, PlayLaunch = " + JsonUtils.serialize(playLaunch));
    }

    private void recoverLaunchUniverses(String customerSpace, String playName, String channelId) {
        log.info("Recovering LaunchUniverses for PlayLaunchChannel ID: " + channelId);

        PlayLaunchChannel channel = playProxy.getChannelById(customerSpace, playName, channelId);
        log.info("PlayLaunchChannel CurrentAccountUniverse before recovery: " + channel.getCurrentLaunchedAccountUniverseTable());
        log.info("PlayLaunchChannel CurrentContactUniverse before recovery: " + channel.getCurrentLaunchedContactUniverseTable());

        channel = playProxy.recoverPlayLaunchChannelLaunchUniverse(customerSpace, playName, channelId, channel);

        log.info("PlayLaunchChannel CurrentAccountUniverse after recovery: " + channel.getCurrentLaunchedAccountUniverseTable());
        log.info("PlayLaunchChannel CurrentContactUniverse after recovery: " + channel.getCurrentLaunchedContactUniverseTable());
    }

    private long getCount(Long object) {
        return object != null ? object.longValue() : 0L;
    }

    private void cleanupIntermediateFiles(JobExecution jobExecution) {
        boolean createRecommendationDataFrame = Boolean.toString(true).equals(getStringValueFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.CREATE_RECOMMENDATION_DATA_FRAME));
        boolean createAddCsvDataFrame = Boolean.toString(true).equals(getStringValueFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.CREATE_ADD_CSV_DATA_FRAME));
        boolean createDeleteCsvDataFrame = Boolean.toString(true).equals(getStringValueFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.CREATE_DELETE_CSV_DATA_FRAME));

        List<String> hdfsIntermediateFiles = new ArrayList<>();
        List<String> hdfsUploadFiles = getListObjectFromContext(jobExecution,
                DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_HDFS_EXPORT_FILE_PATHS, String.class);

        if (createRecommendationDataFrame) {
            hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                    DeltaCampaignLaunchWorkflowConfiguration.RECOMMENDATION_AVRO_HDFS_FILEPATH));
        }
        if (createAddCsvDataFrame) {
            hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                    DeltaCampaignLaunchWorkflowConfiguration.ADD_CSV_EXPORT_AVRO_HDFS_FILEPATH));
        }
        if (createDeleteCsvDataFrame) {
            hdfsIntermediateFiles.add(getStringValueFromContext(jobExecution,
                    DeltaCampaignLaunchWorkflowConfiguration.DELETE_CSV_EXPORT_AVRO_HDFS_FILEPATH));
        }
        if (hdfsUploadFiles != null) {
            hdfsIntermediateFiles.addAll(hdfsUploadFiles);
        }

        log.info("Deleting files: " + Arrays.toString(hdfsIntermediateFiles.toArray()));
        for (String filePath : hdfsIntermediateFiles) {
            if (StringUtils.isBlank(filePath)) {
                continue;
            }
            try {
                 HdfsUtils.rmdir(yarnConfiguration, //
                 filePath.substring(0, filePath.lastIndexOf("/")));
            } catch (Exception ex) {
                log.error("Ignoring error while deleting dir: {}" //
                        + filePath.substring(0, filePath.lastIndexOf("/")), //
                        ex.getMessage());
            }
        }

    }

}
