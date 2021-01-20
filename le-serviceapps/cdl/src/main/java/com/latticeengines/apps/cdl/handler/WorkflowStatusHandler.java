package com.latticeengines.apps.cdl.handler;

import java.util.Map;

import com.latticeengines.apps.cdl.service.PlayLaunchChannelService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

public interface WorkflowStatusHandler {

    default DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        return statusMonitor;
    }

    DataIntegrationEventType getEventType();

    String URL = "url";
    String FOLDER = "dropfolder";

    default void checkStatusMonitorExists(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        if (statusMonitor == null) {
            throw new LedpException(LedpCode.LEDP_40047, new String[] { "null", status.getEventType() });
        }
    }

    default void updateMonitoringStatus(DataIntegrationStatusMonitor statusMonitor, String messageEventType) {
        if (DataIntegrationEventType.canTransit(DataIntegrationEventType.valueOf(statusMonitor.getStatus()),
                DataIntegrationEventType.valueOf(messageEventType))) {
            statusMonitor.setStatus(messageEventType);
        }
    }

    default void saveErrorFileInMonitor(DataIntegrationStatusMonitor statusMonitor, Map<String, String> errorFileMap) {
        if (errorFileMap != null && errorFileMap.containsKey(URL) && errorFileMap.get(URL).contains(FOLDER)) {
            String errorFile = errorFileMap.get(URL);
            statusMonitor.setErrorFile(errorFile.substring(errorFile.indexOf(FOLDER)));
        }
    }

    default void recoverLaunchUniverse(String launchId, PlayLaunchChannelService playLaunchChannelService, PlayLaunchService playLaunchService) {
        PlayLaunchChannel channel = playLaunchService.findPlayLaunchChannelByLaunchId(launchId);
        playLaunchChannelService.recoverLaunchUniverse(channel);
    }

    default void handleAuthenticationState(DataIntegrationStatusMonitorMessage status) {
        if (status == null) {
            throw new LedpException(LedpCode.LEDP_40047, new String[] { "null" });
        }
    };
}
