package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.hibernate.Hibernate;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.AudienceCreationEventDetail;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.ChannelConfig;
import com.latticeengines.domain.exposed.pls.cdl.channel.MarketoChannelConfig;

@Component
public class AudienceCreationWorkflowStatusHandler implements WorkflowStatusHandler {

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.AudienceCreation;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        statusMonitor.setStatus(DataIntegrationEventType.AudienceCreation.toString());

        AudienceCreationEventDetail eventDetail = (AudienceCreationEventDetail) status.getEventDetail();

        switch (statusMonitor.getEntityName()) {
        case "PlayLaunch":
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId());
            if(playLaunch != null) {
                playLaunch.setAudienceId(eventDetail.getAudienceId());
                playLaunch.setAudienceName(eventDetail.getAudienceName());
                ChannelConfig channelConfig = playLaunch.getChannelConfig();
                if (channelConfig != null && channelConfig instanceof MarketoChannelConfig) {
                    Hibernate.initialize(playLaunch.getPlayLaunchChannel());
                    Hibernate.initialize(playLaunch.getPlay());
                    ((MarketoChannelConfig) channelConfig).setAudienceId(eventDetail.getAudienceId());
                    playLaunch.setChannelConfig(channelConfig);
                    PlayLaunchChannel channelConfigChannel = playLaunch.getPlayLaunchChannel();
                    ((MarketoChannelConfig) channelConfigChannel.getChannelConfig()).setAudienceId(eventDetail.getAudienceId());
                    channelConfigChannel.setChannelConfig(channelConfig);
                    playLaunchChannelService.update(playLaunch.getPlay().getName(), channelConfigChannel);
                }
                playLaunchService.update(playLaunch);
            }

        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
