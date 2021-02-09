package com.latticeengines.apps.cdl.handler;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.ExternalSystemAuthenticationService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.domain.exposed.cdl.AuthInvalidatedEventDetail;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.remote.exposed.service.tray.TrayService;

@Component
public class AuthInvalidatedWorkflowStatusHandler implements WorkflowStatusHandler {

    private static final Logger log = LoggerFactory.getLogger(AuthInvalidatedWorkflowStatusHandler.class);

    @Inject
    private ExternalSystemAuthenticationService externalSystemAuthenticationService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private TrayService trayService;

    @Override
    public DataIntegrationEventType getEventType() {
        return DataIntegrationEventType.AuthInvalidated;
    }

    public AuthInvalidatedWorkflowStatusHandler(TrayService trayService,
                LookupIdMappingService lookupIdMappingService,
                ExternalSystemAuthenticationService externalSystemAuthenticationService) {
        this.trayService = trayService;
        this.lookupIdMappingService = lookupIdMappingService;
        this.externalSystemAuthenticationService = externalSystemAuthenticationService;
    }

    @Override
    public void handleAuthenticationState(DataIntegrationStatusMonitorMessage status) {
        log.info("Handling Invalidated Authentication");

        try {
            AuthInvalidatedEventDetail eventDetail = (AuthInvalidatedEventDetail) status.getEventDetail();
            String trayAuthId = eventDetail.getTrayAuthenticationId();
            String trayUserId = eventDetail.getTrayUserId();
            log.info(String.format("Tray Auth ID: %s, Tray User ID: %s", trayAuthId, trayUserId));

            List<ExternalSystemAuthentication> extSysAuths = externalSystemAuthenticationService.findAuthenticationsByTrayAuthId(trayAuthId);
            if (extSysAuths == null || extSysAuths.size() == 0) {
                log.info("ExternalSystemAuthentication not found. Deleting Tray Auth: " + trayAuthId);
                deleteTrayAuth(trayAuthId, trayUserId);
            } else {
                for (ExternalSystemAuthentication extSysAuth : extSysAuths) {
                    handleInvalidatedAuth(extSysAuth, trayAuthId, trayUserId);
                }
            }
        } catch (Exception e) {
            log.error("Failed to disconnect lookupIdMap with error: ", e);
        }
    }

    private void handleInvalidatedAuth(ExternalSystemAuthentication extSysAuth, String trayAuthId, String trayUserId) {
        String extSysAuthId = extSysAuth.getId();
        log.info("Handling lookupIdMap with ExternalSystemAuthentication ID: " + extSysAuthId);
        LookupIdMap lookupIdMap = lookupIdMappingService.getLookupIdMapByExtSysAuth(extSysAuthId);
        if (lookupIdMap == null) {
            log.info("LookupIdMap not found. Deleting Tray Auth: " + trayAuthId);
            deleteTrayAuth(trayAuthId, trayUserId);
        } else {
            String lookupIdMapId = lookupIdMap.getId();
            log.info("Deregistering lookupIdMap ID: " + lookupIdMapId);
            if (lookupIdMap.getIsRegistered()) {
                lookupIdMap.setIsRegistered(false);
                lookupIdMappingService.updateLookupIdMap(lookupIdMapId, lookupIdMap);
            }
        }
    }

    private void deleteTrayAuth(String trayAuthId, String trayUserId) {
        String userToken = trayService.getTrayUserToken(trayUserId);
        trayService.removeAuthenticationById(trayAuthId, userToken);
    }

}
