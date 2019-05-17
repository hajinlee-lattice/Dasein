package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component
public class CompletedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(CompletedWorkflowStatusHandler.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Completed;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {
        
        checkStatusMonitorExists(statusMonitor, status);

        statusMonitor.setEventCompletedTime(status.getEventTime());

        updateMonitoringStatus(statusMonitor, status.getEventType());

        ProgressEventDetail eventDetail = (ProgressEventDetail) status.getEventDetail();

        switch (statusMonitor.getEntityName()) {
        case "PlayLaunch":
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId());
            Long recordsProcessed = eventDetail.getProcessed();
            Long recordsFailed = eventDetail.getFailed();
            Long totalRecords = eventDetail.getTotalRecordsSubmitted();
            if (recordsProcessed.equals(totalRecords)) {
                playLaunch.setLaunchState(LaunchState.Synced);
            } else if (recordsFailed.equals(totalRecords)) {
                playLaunch.setLaunchState(LaunchState.SyncFailed);
            } else {
                playLaunch.setLaunchState(LaunchState.PartialSync);
            }
            playLaunch.setContactsErrored(recordsFailed);
            playLaunch.setContactsDuplicated(totalRecords - recordsProcessed - recordsFailed);
            playLaunchService.update(playLaunch);
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
