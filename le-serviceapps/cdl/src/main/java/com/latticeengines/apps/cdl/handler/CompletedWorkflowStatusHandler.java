package com.latticeengines.apps.cdl.handler;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;

@Component
public class CompletedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(CompletedWorkflowStatusHandler.class);

    private String URL = "url";
    private String FOLDER = "dropfolder";

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Completed;
    }

    public CompletedWorkflowStatusHandler(PlayLaunchService playLaunchService,
                                          PlayLaunchChannelService playLaunchChannelService,
                                          DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr) {
        this.playLaunchService = playLaunchService;
        this.playLaunchChannelService = playLaunchChannelService;
        this.dataIntegrationStatusMonitoringEntityMgr = dataIntegrationStatusMonitoringEntityMgr;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        statusMonitor.setEventCompletedTime(status.getEventTime());

        updateMonitoringStatus(statusMonitor, status.getEventType());

        String launchId = statusMonitor.getEntityId();
        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId, false);
        if (playLaunch == null) {
            log.error("DataIntegrationStatusMonitor NOT updated: Entity " + launchId
                    + "is not returning the playLaunch.");
            return statusMonitor;
        }

        ProgressEventDetail eventDetail = (ProgressEventDetail) status.getEventDetail();
        updatePlayLaunch(playLaunch, eventDetail, launchId);
        saveErrorFile(statusMonitor, eventDetail, playLaunch.getLaunchState());

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

    private void updatePlayLaunch(PlayLaunch playLaunch, ProgressEventDetail eventDetail, String launchId) {
        if (eventDetail == null) {
            return;
        }

        Long totalRecords = eventDetail.getTotalRecordsSubmitted();
        Long recordsProcessed = eventDetail.getProcessed();
        Long recordsFailed = eventDetail.getFailed();
        Long duplicatedRecords = eventDetail.getDuplicates() == null ? 0L : eventDetail.getDuplicates();
        Long processedAndFailed = recordsFailed + duplicatedRecords;

        if (recordsFailed.equals(totalRecords) || processedAndFailed.equals(totalRecords)) {
            playLaunch.setLaunchState(LaunchState.SyncFailed);
        } else {
            if (recordsProcessed.equals(totalRecords)) {
                playLaunch.setLaunchState(LaunchState.Synced);
            } else {
                playLaunch.setLaunchState(LaunchState.PartialSync);
            }
        }

        log.info("Channel Config for launch ID " + playLaunch.getLaunchId() + ": "
                + JsonUtils.serialize(playLaunch.getChannelConfig()));
        if (playLaunch.getChannelConfig() != null
                && playLaunch.getChannelConfig().getAudienceType() == AudienceType.ACCOUNTS) {
            playLaunch.setAccountsErrored(recordsFailed);
            playLaunch.setAccountsDuplicated(duplicatedRecords);
        } else {
            playLaunch.setContactsErrored(recordsFailed);
            playLaunch.setContactsDuplicated(duplicatedRecords);
        }

        updateLaunchUniverse(playLaunch.getLaunchState(), launchId);
        playLaunchService.update(playLaunch);
    }

    private void saveErrorFile(DataIntegrationStatusMonitor statusMonitor, ProgressEventDetail eventDetail, LaunchState launchState) {
        if (!launchState.equals(LaunchState.PartialSync)) {
            return;
        }

        Map<String, String> errorFileMap = eventDetail.getErrorFile();

        if (errorFileMap != null && errorFileMap.containsKey(URL) && errorFileMap.get(URL).contains(FOLDER)) {
            String errorFile = errorFileMap.get(URL);
            statusMonitor.setErrorFile(errorFile.substring(errorFile.indexOf(FOLDER)));
        }
    }

    private void updateLaunchUniverse(LaunchState launchState, String launchId) {
        if (launchState.equals(LaunchState.SyncFailed)) {
            recoverLaunchUniverse(launchId);
        } else {
            updatePreviousLaunchUniverse(launchId);
        }
    }

    private void updatePreviousLaunchUniverse(String launchId) {
        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
        playLaunchChannelService.updatePreviousLaunchedAccountUniverseWithCurrent(channel);
        playLaunchChannelService.updatePreviousLaunchedContactUniverseWithCurrent(channel);
    }

    private void recoverLaunchUniverse(String launchId) {
        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
        playLaunchChannelService.updateCurrentLaunchedAccountUniverseWithPrevious(channel);
        playLaunchChannelService.updateCurrentLaunchedContactUniverseWithPrevious(channel);
    }
}
