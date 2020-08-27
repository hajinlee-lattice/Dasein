package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.AudienceEventDetail;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component
public class AudienceSizeUpdateWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(AudienceSizeUpdateWorkflowStatusHandler.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.AudienceSizeUpdate;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        statusMonitor.setStatus(DataIntegrationEventType.AudienceSizeUpdate.toString());

        AudienceEventDetail eventDetail = (AudienceEventDetail) status.getEventDetail();

        if (statusMonitor.getEntityName().equals("PlayLaunch")) {
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId(), false);
            if (playLaunch == null) {
                log.error("DataIntegrationStatusMonitor NOT updated: Entity " + statusMonitor.getEntityId()
                        + "is not returning the playLaunch.");
                return statusMonitor;
            }

            playLaunch.setAudienceSize(eventDetail.getAudienceSize());
            playLaunch.setMatchedCount(eventDetail.getMatchedCount());
            playLaunchService.update(playLaunch);
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
