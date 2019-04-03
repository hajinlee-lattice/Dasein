package com.latticeengines.apps.cdl.handler;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.FailedEventDetail;

@Component
public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

    private String URL = "url";

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

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

        Map<String, String> errorFileMap = eventDetail.getErrorFile();

        if (errorFileMap != null && errorFileMap.containsKey(URL)) {
            statusMonitor.setErrorFile(errorFileMap.get(URL));
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
