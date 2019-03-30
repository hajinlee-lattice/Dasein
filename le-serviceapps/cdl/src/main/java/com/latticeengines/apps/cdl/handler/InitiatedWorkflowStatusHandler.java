package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.DataIntegrationStatusMonitoringEntityMgr;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.PlayLaunch;

@Component
public class InitiatedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(InitiatedWorkflowStatusHandler.class);


    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private DataIntegrationStatusMonitoringEntityMgr dataIntegrationStatusMonitoringEntityMgr;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.Initiated;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        updateMonitoringStatus(statusMonitor, DataIntegrationEventType.Initiated.toString());

        statusMonitor.setEventStartedTime(status.getEventTime());

        switch (statusMonitor.getEntityName()) {
        case "PlayLaunch":
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId());
            if (playLaunch == null) {
                if (MultiTenantContext.getTenant() != null) {
                    log.info("MultiTenantContext tenant name: " + MultiTenantContext.getTenant().getName());
                }
                log.info("statusMonitor: " + JsonUtils.serialize(status));
            }
            playLaunch.setLaunchState(LaunchState.Syncing);
            playLaunchService.update(playLaunch);
        }

        return dataIntegrationStatusMonitoringEntityMgr.updateStatus(statusMonitor);
    }

}
