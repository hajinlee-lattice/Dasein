package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.FacebookChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.LinkedInChannelConfig;

@Component
public class CompletedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(CompletedWorkflowStatusHandler.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Completed;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        
        checkStatusMonitorExists(statusMonitor, status);

        statusMonitor.setEventCompletedTime(status.getEventTime());

        updateMonitoringStatus(statusMonitor, status.getEventType());

        ProgressEventDetail eventDetail = (ProgressEventDetail) status.getEventDetail();

        switch (statusMonitor.getEntityName()) {
        case "PlayLaunch":
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId());
            Long recordsProcessed = eventDetail.getProcessed();
            Long recordsFailed = eventDetail.getFailed();
            Long totalRecords = eventDetail.getTotalRecordsSubmitted();
            Long duplicatedRecords = eventDetail.getDuplicates();
            if (recordsProcessed.equals(totalRecords)) {
                playLaunch.setLaunchState(LaunchState.Synced);
            } else if (recordsFailed.equals(totalRecords)) {
                playLaunch.setLaunchState(LaunchState.SyncFailed);
            } else {
                playLaunch.setLaunchState(LaunchState.PartialSync);
            }
            CDLExternalSystemName systemName = playLaunch.getPlayLaunchChannel() != null
                    && playLaunch.getPlayLaunchChannel().getLookupIdMap() != null
                            ? playLaunch.getPlayLaunchChannel().getLookupIdMap().getExternalSystemName()
                            : null;
            log.info("Channel Config for launch ID " + playLaunch.getLaunchId() + ": "
                    + JsonUtils.serialize(playLaunch.getChannelConfig()));
            if (playLaunch.getChannelConfig() != null
                    && checkAudienceType(systemName, playLaunch.getChannelConfig(), AudienceType.ACCOUNTS) == true) {
                playLaunch.setAccountsErrored(recordsFailed);
                playLaunch.setAccountsDuplicated(duplicatedRecords);
            } else {
                playLaunch.setContactsErrored(recordsFailed);
                playLaunch.setContactsDuplicated(duplicatedRecords);
            }

            playLaunch.setAudienceSize(eventDetail.getAudienceSize());
            playLaunch.setMatchedCount(eventDetail.getMatchedCount());

            playLaunchService.update(playLaunch);
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

    private Boolean checkAudienceType(CDLExternalSystemName systemName, ChannelConfig channelConfig,
            AudienceType audienceType) {
        switch (systemName) {
        case Marketo:
        case GoogleAds:
            return audienceType == AudienceType.CONTACTS;
        case LinkedIn:
            LinkedInChannelConfig linkedInconfig = (LinkedInChannelConfig) channelConfig;
            return linkedInconfig.getAudienceType() == audienceType;
        case Facebook:
            FacebookChannelConfig fbConfig = (FacebookChannelConfig) channelConfig;
            return fbConfig.getAudienceType() == audienceType;
        default:
            return null;
        }
    }

}
