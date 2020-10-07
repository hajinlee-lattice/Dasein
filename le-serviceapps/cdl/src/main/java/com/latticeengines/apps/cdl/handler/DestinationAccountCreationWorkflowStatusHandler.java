package com.latticeengines.apps.cdl.handler;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.domain.exposed.cdl.AccountEventDetail;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;

@Component
public class DestinationAccountCreationWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(DestinationAccountCreationWorkflowStatusHandler.class);

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.DestinationAccountCreation;
    }

    @Override
    public DataIntegrationStatusMonitor handleWorkflowState(DataIntegrationStatusMonitor statusMonitor,
            DataIntegrationStatusMonitorMessage status) {

        checkStatusMonitorExists(statusMonitor, status);

        AccountEventDetail eventDetail = (AccountEventDetail) status.getEventDetail();

        if (statusMonitor.getEntityName().equals("PlayLaunch")) {
            PlayLaunch playLaunch = playLaunchService.findByLaunchId(statusMonitor.getEntityId(), true);
            if (playLaunch == null) {
                log.error("DataIntegrationStatusMonitor NOT updated: Entity " + statusMonitor.getEntityId()
                        + "is not returning the playLaunch.");
                return statusMonitor;
            }
            PlayLaunchChannel playLaunchChannel = playLaunch.getPlayLaunchChannel();
            LookupIdMap lookupIdMap = playLaunchChannel.getLookupIdMap();
            lookupIdMap.setOrgId(eventDetail.getAccountId());
            
            lookupIdMappingService.updateLookupIdMap(lookupIdMap.getId(), lookupIdMap);
        }

        return statusMonitor;
    }

}
