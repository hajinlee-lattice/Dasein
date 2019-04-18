package com.latticeengines.apps.cdl.handler;

import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public interface WorkflowStatusHandler {

    default DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        return statusMonitor;
    }

    DataIntegrationEventType getEventType();

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
}
