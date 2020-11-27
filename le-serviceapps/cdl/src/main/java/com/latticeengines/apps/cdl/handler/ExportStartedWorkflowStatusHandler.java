package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component
public class ExportStartedWorkflowStatusHandler implements WorkflowStatusHandler {
    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.ExportStart;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        updateMonitoringStatus(statusMonitor, DataIntegrationEventType.ExportStart.toString());

        if (statusMonitor.getStatus().equals(DataIntegrationEventType.ExportStart.toString())) {

            statusMonitor.setEventStartedTime(status.getEventTime());

            if (statusMonitor.getEntityName().equals("PlayLaunch")) {
                PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId(), true);
                playLaunch.setLaunchState(LaunchState.Syncing);
                playLaunch.setTapType(playLaunch.getPlay().getTapType());
                playLaunchService.update(playLaunch);
            }
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
