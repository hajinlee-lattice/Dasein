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
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RecordsStats;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.proxy.exposed.pls.EmailProxy;

@Component
public class CompletedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(CompletedWorkflowStatusHandler.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private EmailProxy emailProxy;

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
        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId, true);
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
        // Assume this is always true:
        // totalRecords = recordsProcessed + recordsFailed + duplicatedRecords
        Long totalRecords = eventDetail.getTotalRecordsSubmitted();
        Long recordsProcessed = eventDetail.getProcessed();
        Long recordsFailed = eventDetail.getFailed();
        Long duplicatedRecords = eventDetail.getDuplicates() == null ? 0L : eventDetail.getDuplicates();
        Long recordsDuplicateAndFailed = recordsFailed + duplicatedRecords;

        if (recordsProcessed.equals(totalRecords)) {
            playLaunch.setLaunchState(LaunchState.Synced);
        } else if (recordsFailed.equals(totalRecords) || recordsDuplicateAndFailed.equals(totalRecords)) {
            playLaunch.setLaunchState(LaunchState.SyncFailed);
        } else {
            playLaunch.setLaunchState(LaunchState.PartialSync);
        }

        sendEmailIfSyncFailed(playLaunch);

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

        RecordsStats recordsStats = playLaunch.getRecordsStats();

        if (recordsStats == null) {
            recordsStats = new RecordsStats();
        }

        recordsStats.setRecordsFromLattice(totalRecords);
        recordsStats.setRecordsReceivedByTray(totalRecords);
        recordsStats.setRecordsToAcxiom(totalRecords);
        recordsStats.setLatticeRecordsSucceedToDestination(recordsProcessed);
        recordsStats.setLatticeRecordsFailToDestination(recordsFailed);
        playLaunch.setRecordsStats(recordsStats);

        playLaunchService.update(playLaunch);
    }

    private void saveErrorFile(DataIntegrationStatusMonitor statusMonitor, ProgressEventDetail eventDetail, LaunchState launchState) {
        if (!launchState.equals(LaunchState.PartialSync)) {
            return;
        }

        Map<String, String> errorFileMap = eventDetail.getErrorFile();
        saveErrorFileInMonitor(statusMonitor, errorFileMap);
    }

    private void updateLaunchUniverse(LaunchState launchState, String launchId) {
        if (launchState.equals(LaunchState.SyncFailed)) {
            recoverLaunchUniverse(launchId, playLaunchChannelService, playLaunchService);
        } else {
            updatePreviousLaunchUniverse(launchId);
        }
    }

    private void updatePreviousLaunchUniverse(String launchId) {
        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
        playLaunchChannelService.updatePreviousLaunchedAccountUniverseWithCurrent(channel);
        playLaunchChannelService.updatePreviousLaunchedContactUniverseWithCurrent(channel);
    }

    private void sendEmailIfSyncFailed(PlayLaunch playLaunch) {
        if (playLaunch.getLaunchState().equals(LaunchState.SyncFailed)) {
            PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(playLaunch.getId());
            try {
                emailProxy.sendPlayLaunchErrorEmail(
                        MultiTenantContext.getTenant().getId(), channel.getUpdatedBy(), playLaunch);
            } catch (Exception e) {
                log.error("Can not send play launch failed email: " + e.getMessage());
            }
        }

    }
}
