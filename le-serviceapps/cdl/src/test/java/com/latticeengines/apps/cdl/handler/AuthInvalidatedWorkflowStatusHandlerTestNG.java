package com.latticeengines.apps.cdl.handler;

import java.util.Collections;
import java.util.Date;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.ExternalSystemAuthenticationService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.domain.exposed.cdl.AuthInvalidatedEventDetail;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.pls.ExternalSystemAuthentication;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.remote.exposed.service.tray.TrayService;
import com.latticeengines.security.exposed.service.TenantService;

public class AuthInvalidatedWorkflowStatusHandlerTestNG extends StatusHandlerTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(AuthInvalidatedWorkflowStatusHandlerTestNG.class);
    private static String TEST_TRAY_ID_1 = "TEST_TRAY_1_";
    private static String TEST_TRAY_ID_2 = "TEST_TRAY_2_";
    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();
    private static String TEST_TENANT = "Test Lattice Tenant";

    @Inject
    private ExternalSystemAuthenticationService externalSystemAuthenticationService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private TrayService trayService;

    @Inject
    TenantService tenantService;

    private Tenant tenant1;
    private LookupIdMap lookupIdMap1;
    private LookupIdMap lookupIdMap2;
    private DataIntegrationStatusMonitorMessage statusMessage;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        setupTestEnvironment();
        addAnotherTenant();
        lookupIdMap1 = createLookupIdMap();
        lookupIdMap2 = createLookupIdMap();
        statusMessage = createAuthStatusMessage(TEST_TRAY_ID_2);
    }

    @Test(groups = "functional")
    public void testAuthInvalidatedWorkflowStatusHandler() {
        String lookupIdMapId2 = lookupIdMap2.getId();
        createExtSysAuth(lookupIdMap1, TEST_TRAY_ID_1);
        log.info("lookupIdMap1 tenant: " + lookupIdMap1.getTenant().toString());

        lookupIdMap2.setTenant(tenant1);
        createExtSysAuth(lookupIdMap2, TEST_TRAY_ID_2);
        log.info("lookupIdMap2 tenant: " + lookupIdMap2.getTenant().toString());

        Assert.assertTrue(lookupIdMap2.getIsRegistered());

        String statusJson = JsonUtils.serialize(statusMessage);
        log.info("STATUS SERIALIZED " + statusJson);

        AuthInvalidatedWorkflowStatusHandler handler = new AuthInvalidatedWorkflowStatusHandler(trayService,
                lookupIdMappingService, externalSystemAuthenticationService);
        handler.handleAuthenticationState(statusMessage);

        RetryTemplate retry = RetryUtils.getRetryTemplate(5, //
                Collections.singleton(AssertionError.class), null);

        retry.execute(context -> {
            LookupIdMap updatedLookupIdMap = lookupIdMappingService.getLookupIdMap(lookupIdMapId2);
            Assert.assertFalse(updatedLookupIdMap.getIsRegistered());
            return true;
        });
    }

    private AuthInvalidatedEventDetail createAuthEventDetail(String authName) {
        AuthInvalidatedEventDetail eventDetail = new AuthInvalidatedEventDetail();
        eventDetail.setTrayAuthenticationId(authName + CURRENT_TIME_MILLIS);
        return eventDetail;
    }

    private ExternalSystemAuthentication createExtSysAuth(LookupIdMap lookupIdMap, String authName) {
        ExternalSystemAuthentication extSysAuth = new ExternalSystemAuthentication();
        extSysAuth.setTrayAuthenticationId(authName + CURRENT_TIME_MILLIS);
        extSysAuth.setLookupIdMap(lookupIdMap);
        externalSystemAuthenticationService.createAuthentication(extSysAuth);
        return extSysAuth;
    }

    private DataIntegrationStatusMonitorMessage createAuthStatusMessage(String authName) {
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setMessageType(MessageType.Information.toString());
        statusMessage.setEventType(DataIntegrationEventType.AuthInvalidated.toString());
        statusMessage.setEventTime(new Date());
        statusMessage.setEventDetail(createAuthEventDetail(authName));

        return statusMessage;
    }

    private void addAnotherTenant() {
        tenant1 = tenantService.findByTenantName(TEST_TENANT);
        if (tenant1 != null) {
            testBed.deleteTenant(tenant1);
        }
        tenant1 = new Tenant();
        tenant1.setId(TEST_TENANT);
        tenant1.setName(TEST_TENANT);
        testBed.createTenant(tenant1);
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
        try {
            testBed.deleteTenant(mainTestTenant);
            testBed.deleteTenant(tenant1);
        } catch (Exception ignore) {
            // tenant does not exist
        }
    }
}
