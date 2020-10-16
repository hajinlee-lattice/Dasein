package com.latticeengines.apps.cdl.handler;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.MapUtils;
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
import com.latticeengines.domain.exposed.cdl.FailedEventDetail;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.cdl.channel.AudienceType;
import com.latticeengines.proxy.exposed.pls.EmailProxy;

@Component
public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(FailedWorkflowStatusHandler.class);

    private String MSG = "message";

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

    @Inject
    private EmailProxy emailProxy;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Failed;
    }

    public FailedWorkflowStatusHandler(PlayLaunchService playLaunchService,
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
        statusMonitor.setStatus(DataIntegrationEventType.Failed.toString());

        handleErrorObject(statusMonitor, status);

        String launchId = statusMonitor.getEntityId();
        updatePlayLaunchAndSendEmail(launchId);

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

    private void handleErrorObject(DataIntegrationStatusMonitor statusMonitor, DataIntegrationStatusMonitorMessage status) {
        FailedEventDetail eventDetail = (FailedEventDetail) status.getEventDetail();
        String messageType = status.getMessageType();

        if (eventDetail == null) {
            return;
        }

        if (MessageType.Information.equals(MessageType.valueOf(messageType))) {
            saveErrorFile(eventDetail, statusMonitor);
        } else if (MessageType.Event.equals(MessageType.valueOf(messageType))) {
            saveErrorMessage(eventDetail, statusMonitor);
        }
    }

    private void saveErrorNumber(PlayLaunch playLaunch) {
        Long totalAccounts = playLaunch.getAccountsLaunched();
        Long totalContacts = playLaunch.getContactsLaunched();

        if (playLaunch.getChannelConfig().getAudienceType() == AudienceType.ACCOUNTS) {
            playLaunch.setAccountsErrored(totalAccounts);
        } else {
            playLaunch.setContactsErrored(totalContacts);
        }
    }

    private void saveErrorFile(FailedEventDetail eventDetail, DataIntegrationStatusMonitor statusMonitor) {
        Map<String, String> errorFileMap = eventDetail.getErrorFile();
        saveErrorFileInMonitor(statusMonitor, errorFileMap);
    }


    private void saveErrorMessage(FailedEventDetail eventDetail, DataIntegrationStatusMonitor statusMonitor) {
        if (MapUtils.isNotEmpty(eventDetail.getError()) && eventDetail.getError().containsKey(MSG)) {
            Map<String, Object> errorMap = eventDetail.getError();
            statusMonitor.setErrorMessage(JsonUtils.serialize(errorMap.get(MSG)));
        }
        log.info(String.format("WorkflowRequestId %s failed with message: %s", statusMonitor.getWorkflowRequestId(),
                JsonUtils.serialize(eventDetail)));
    }

    private void updatePlayLaunchAndSendEmail(String launchId) {
        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId, false);

        saveErrorNumber(playLaunch);
        playLaunch.setLaunchState(LaunchState.SyncFailed);
        recoverLaunchUniverse(launchId, playLaunchChannelService, playLaunchService);
        playLaunchService.update(playLaunch);

        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(playLaunch.getId());
        try {
            emailProxy.sendPlayLaunchErrorEmail(
                    MultiTenantContext.getTenant().getId(), channel.getUpdatedBy(), playLaunch);
        } catch (Exception e) {
            log.error("Can not send play launch failed email: " + e.getMessage());
        }
    }
}
