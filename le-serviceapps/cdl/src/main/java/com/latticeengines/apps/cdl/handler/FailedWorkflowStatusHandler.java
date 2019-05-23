package com.latticeengines.apps.cdl.handler;

import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.FailedEventDetail;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component
public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(FailedWorkflowStatusHandler.class);

    private String URL = "url";
    private String MSG = "message";

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Failed;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        statusMonitor.setStatus(DataIntegrationEventType.Failed.toString());

        FailedEventDetail eventDetail = (FailedEventDetail) status.getEventDetail();

        String messageType = status.getMessageType();

        if (MessageType.Information.equals(MessageType.valueOf(messageType)) && eventDetail != null) {
            Map<String, String> errorFileMap = eventDetail.getErrorFile();

            if (errorFileMap != null && errorFileMap.containsKey(URL)
                    && errorFileMap.get(URL).indexOf("dropfolder") >= 0) {
                String errorFile = errorFileMap.get(URL);
                statusMonitor.setErrorFile(errorFile.substring(errorFile.indexOf("dropfolder")));
            }
        } else if (MessageType.Event.equals(MessageType.valueOf(messageType))) {
            Map<String, Object> errorMap = eventDetail.getError();
            if (errorMap != null && errorMap.containsKey(MSG)) {
                statusMonitor.setErrorMessage(JsonUtils.serialize(errorMap.get(MSG)));
            }
            log.info(String.format("WorkflowRequestId %s failed with message: %s", statusMonitor.getWorkflowRequestId(),
                    JsonUtils.serialize(eventDetail)));
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId());
            playLaunch.setLaunchState(LaunchState.SyncFailed);
            playLaunchService.update(playLaunch);
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
