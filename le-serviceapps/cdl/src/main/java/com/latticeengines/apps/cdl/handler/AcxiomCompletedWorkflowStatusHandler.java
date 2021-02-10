package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.AcxiomCompleted;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RecordsStats;

@Component
public class AcxiomCompletedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(AcxiomCompletedWorkflowStatusHandler.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.AcxiomCompleted;
    }

    public AcxiomCompletedWorkflowStatusHandler(PlayLaunchService playLaunchService) {
        this.playLaunchService = playLaunchService;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        AcxiomCompleted eventDetail = (AcxiomCompleted) status.getEventDetail();

        if (statusMonitor.getEntityName().equals("PlayLaunch")) {
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId(), false);
            if (playLaunch == null) {
                log.error("DataIntegrationStatusMonitor NOT updated: Entity " + statusMonitor.getEntityId()
                        + "is not returning the playLaunch.");
                return statusMonitor;
            }

            RecordsStats recordsStats = playLaunch.getRecordsStats();

            if (recordsStats == null) {
                recordsStats = new RecordsStats();
            }

            recordsStats.setAcxiomRecordsSucceedToDestination(eventDetail.getProcessedCount());
            recordsStats.setAcxiomRecordsFailToDestination(eventDetail.getFailedCount());
            playLaunch.setRecordsStats(recordsStats);
            playLaunchService.update(playLaunch);
        }

        return statusMonitor;
    }

}
