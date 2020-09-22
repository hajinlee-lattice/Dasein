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
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.FailedEventDetail;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component
public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(FailedWorkflowStatusHandler.class);

    private String URL = "url";
    private String MSG = "message";

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayLaunchChannelService playLaunchChannelService;

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
        updatePlayLaunch(launchId);
        recoverLaunchUniverse(launchId);

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

    private void saveErrorFile(FailedEventDetail eventDetail, DataIntegrationStatusMonitor statusMonitor) {
        Map<String, String> errorFileMap = eventDetail.getErrorFile();

        if (errorFileMap != null && errorFileMap.containsKey(URL) && errorFileMap.get(URL).contains("dropfolder")) {
            String errorFile = errorFileMap.get(URL);
            statusMonitor.setErrorFile(errorFile.substring(errorFile.indexOf("dropfolder")));
        }
    }

    private void saveErrorMessage(FailedEventDetail eventDetail, DataIntegrationStatusMonitor statusMonitor) {
        if (MapUtils.isNotEmpty(eventDetail.getError()) && eventDetail.getError().containsKey(MSG)) {
            Map<String, Object> errorMap = eventDetail.getError();
            statusMonitor.setErrorMessage(JsonUtils.serialize(errorMap.get(MSG)));
        }
        log.info(String.format("WorkflowRequestId %s failed with message: %s", statusMonitor.getWorkflowRequestId(),
                JsonUtils.serialize(eventDetail)));
    }

    private void updatePlayLaunch(String launchId) {
        PlayLaunch playLaunch = playLaunchService.findByLaunchId(launchId, false);
        playLaunch.setLaunchState(LaunchState.SyncFailed);
        playLaunchService.update(playLaunch);
    }

    private void recoverLaunchUniverse(String launchId) {
        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
        playLaunchChannelService.updateCurrentLaunchedAccountUniverseWithPrevious(channel);
        playLaunchChannelService.updateCurrentLaunchedContactUniverseWithPrevious(channel);
    }
}
