package com.latticeengines.apps.cdl.handler;

import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

public interface WorkflowStatusHandler {

    public String PLAY_LAUNCH = PlayLaunch.class.getSimpleName();

    public default DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        return statusMonitor;
    };

    public DataIntegrationEventType getEventType();

    public default void checkStatusMonitorExists(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        if (statusMonitor == null) {
            throw new LedpException(LedpCode.LEDP_40047, new String[] { "null", status.getEventType() });
        }
    }

    public default void updateMonitoringStatus(DataIntegrationStatusMonitor statusMonitor, String messageEventType) {
        if (DataIntegrationEventType.canTransit(DataIntegrationEventType.valueOf(statusMonitor.getStatus()),
                DataIntegrationEventType.valueOf(messageEventType))) {
            statusMonitor.setStatus(messageEventType);
        }
    }
}
