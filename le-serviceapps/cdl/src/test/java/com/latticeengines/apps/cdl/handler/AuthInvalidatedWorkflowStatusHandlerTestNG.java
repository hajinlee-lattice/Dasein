package com.latticeengines.apps.cdl.handler;

import java.util.Collections;
import java.util.Date;

import javax.inject.Inject;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ExternalSystemAuthenticationService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.AuthInvalidatedEventDetail;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.remote.exposed.service.tray.TrayService;

public class AuthInvalidatedWorkflowStatusHandlerTestNG extends StatusHandlerTestNGBase {

    private static String TEST_TRAY_ID = "TEST_TRAY_";
    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    @Inject
    private ExternalSystemAuthenticationService externalSystemAuthenticationService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private TrayService trayService;

    private LookupIdMap lookupIdMap;
    private DataIntegrationStatusMonitorMessage statusMessage;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        lookupIdMap = createLookupIdMap();
        statusMessage = createAuthStatusMessage();
        createExtSysAuth(lookupIdMap);
    }

    @Test(groups = "functional")
    public void testAuthInvalidatedWorkflowStatusHandler() {
        String lookupIdMapId = lookupIdMap.getId();
        lookupIdMap.setIsRegistered(true);
        lookupIdMappingService.updateLookupIdMap(lookupIdMapId, lookupIdMap);

        AuthInvalidatedWorkflowStatusHandler handler = new AuthInvalidatedWorkflowStatusHandler(trayService,
                lookupIdMappingService, externalSystemAuthenticationService);
        handler.handleAuthenticationState(statusMessage);

        //trayService.getTrayUserToken("9a6ce3a7-f4a3-48fc-a016-7d905c989a72");

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);

        retry.execute(context -> {
            LookupIdMap updatedLookupIdMap = lookupIdMappingService.getLookupIdMap(lookupIdMapId);
            Assert.assertFalse(updatedLookupIdMap.getIsRegistered());
            return true;
        });
    }

    private AuthInvalidatedEventDetail createAuthEventDetail() {
        AuthInvalidatedEventDetail eventDetail = new AuthInvalidatedEventDetail();
        eventDetail.setTrayAuthenticationId(TEST_TRAY_ID + CURRENT_TIME_MILLIS);
        return eventDetail;
    }

    private ExternalSystemAuthentication createExtSysAuth(LookupIdMap lookupIdMap) {
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setTrayAuthenticationId(TEST_TRAY_ID + CURRENT_TIME_MILLIS);
        extSysAuth.setLookupIdMap(lookupIdMap);
        externalSystemAuthenticationService.createAuthentication(extSysAuth);
        return extSysAuth;
    }

    private DataIntegrationStatusMonitorMessage createAuthStatusMessage() {
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setMessageType(MessageType.Information.toString());
        statusMessage.setEventType(DataIntegrationEventType.AuthInvalidated.toString());
        statusMessage.setEventTime(new Date());
        statusMessage.setEventDetail(createAuthEventDetail());

        return statusMessage;
    }
}
