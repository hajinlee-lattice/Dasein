package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.FailedEventDetail;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component
public class FailedWorkflowStatusHandler implements WorkflowStatusHandler {

    @Inject
    private PlayLaunchService playLaunchService;

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

        statusMonitor.setErrorFile(eventDetail.getErrorFile());

        switch (statusMonitor.getEntityName()) {
        case "PlayLaunch":
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId());
            playLaunch.setLaunchState(LaunchState.Failed);
            playLaunchService.update(playLaunch);
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
