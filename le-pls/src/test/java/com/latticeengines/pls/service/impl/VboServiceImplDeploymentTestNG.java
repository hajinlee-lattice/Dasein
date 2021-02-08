package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

import javax.inject.Inject;

import org.apache.commons.lang3.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.dcp.vbo.VboUserSeatUsageEvent;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.pls.UserUpdateResponse;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.VboService;


/**
 * $ dpltc deploy -p dcp && runtc
 */

public class VboServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(VboServiceImplDeploymentTestNG.class);

    private static final String SUBSCRIBER_NUMBER_OPEN = "500118856";
    private static final String SUBSCRIBER_NUMBER_FULL = "500118852";
    private static final String SUBSCRIBER_NUMBER_OVERFLOW = "500118852";
    private static final String INTERNAL_USER_EMAIL = "build@lattice-engines.com";
    private static final String EXTERNAL_USER_EMAIL = "build.lattice.engines@gmail.com";
    private static final String EXTERNAL_USER_EMAIL2 = "test_user_usageevent2@gmail.com";
    private static final String TEST_SUBSCRIBER_EMAIL = "testDCP1@outlook.com";

    private static final String FORBIDDEN_MSG = "403 FORBIDDEN";
    private static final Date CURRENT_DATE = new Date();

    @Inject
    private TenantService tenantService;

    @Inject
    private UserService userService;

    @Inject
    private VboService vboService;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.DCP);
        MultiTenantContext.setTenant(mainTestTenant);
        userService.deleteUserByEmail(INTERNAL_USER_EMAIL);
        userService.deleteUserByEmail(EXTERNAL_USER_EMAIL);
        userService.deleteUserByEmail(EXTERNAL_USER_EMAIL2);
        ensureSubscriberSeatState();
    }

    @AfterTest(groups = "deployment")
    public void cleanupUsers() {
        userService.deleteUserByEmail(INTERNAL_USER_EMAIL);
        userService.deleteUserByEmail(EXTERNAL_USER_EMAIL);
        userService.deleteUserByEmail(EXTERNAL_USER_EMAIL2);
    }

    @Test(groups = "deployment")
    public void testRegisterInternalEmail() throws InterruptedException {
        String[] subscriberNumbers = {SUBSCRIBER_NUMBER_OPEN, SUBSCRIBER_NUMBER_FULL};
        for (String sn : subscriberNumbers) {
            switchSubscriber(sn);

            // register internal user: assert success; no meter change
            register(INTERNAL_USER_EMAIL, true);

            // delete internal user: assert success; no meter change
            delete(INTERNAL_USER_EMAIL, true);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = {"testRegisterInternalEmail"})
    public void testRegisterExternalEmail() throws InterruptedException {
        // subscriber with open seats: assert success, meter change
        switchSubscriber(SUBSCRIBER_NUMBER_OPEN);
        register(EXTERNAL_USER_EMAIL, true);
        delete(EXTERNAL_USER_EMAIL, true);

        // subscriber at limit: assert fail, no meter change
        switchSubscriber(SUBSCRIBER_NUMBER_FULL);
        register(EXTERNAL_USER_EMAIL, false);
        delete(EXTERNAL_USER_EMAIL, false);
    }

    @Test(groups = "deployment", dependsOnMethods = {"testRegisterExternalEmail"})
    public void testUpdateInternalEmail() throws InterruptedException {
        String[] subscriberNumbers = {SUBSCRIBER_NUMBER_OPEN, SUBSCRIBER_NUMBER_FULL};
        for (String sn : subscriberNumbers) {
            switchSubscriber(sn);

            // update new internal user: assert success, meter change
            userService.createUser(INTERNAL_USER_EMAIL, createUserReg(INTERNAL_USER_EMAIL));
            update(INTERNAL_USER_EMAIL, true, AccessLevel.EXTERNAL_USER, true);

            delete(INTERNAL_USER_EMAIL, true);
        }
    }

    @Test(groups = "deployment", dependsOnMethods = {"testUpdateInternalEmail"})
    public void testUpdateExternalEmail() throws InterruptedException {
        // subscriber with open seats, new external user: assert success, meter change
        switchSubscriber(SUBSCRIBER_NUMBER_OPEN);
        userService.createUser(EXTERNAL_USER_EMAIL, createUserReg(EXTERNAL_USER_EMAIL));
        update(EXTERNAL_USER_EMAIL, true, AccessLevel.EXTERNAL_USER, true);

        // existing external user: assert success, no meter change after update
        update(EXTERNAL_USER_EMAIL, true, AccessLevel.EXTERNAL_ADMIN, false);
        delete(EXTERNAL_USER_EMAIL, true);

        // subscriber at limit, new external user: assert 403, seat count unchanged
        switchSubscriber(SUBSCRIBER_NUMBER_FULL);
        userService.createUser(EXTERNAL_USER_EMAIL, createUserReg(EXTERNAL_USER_EMAIL));
        update(EXTERNAL_USER_EMAIL, false, AccessLevel.EXTERNAL_USER, true);
        delete(EXTERNAL_USER_EMAIL, false);
    }

    @Test(groups = "deployment", dependsOnMethods = {"testUpdateExternalEmail"})
    public void testRegisterExternalEmailAfterDecrement() throws InterruptedException {
        // subscriber at limit: assert fail, no meter change
        VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();
        switchSubscriber(SUBSCRIBER_NUMBER_FULL);

        register(EXTERNAL_USER_EMAIL2, false);

        int available = getAvailableSeats( SUBSCRIBER_NUMBER_FULL);

        while ( available <1 ) {
            sendUsageEvent(VboUserSeatUsageEvent.FeatureURI.STDEC, SUBSCRIBER_NUMBER_FULL);
            available++;
        }
        Thread.sleep(3000);
        int open = getAvailableSeats( SUBSCRIBER_NUMBER_FULL);

        register(EXTERNAL_USER_EMAIL2, true);
        delete(EXTERNAL_USER_EMAIL2, true);

    }

    private void switchSubscriber(String subscriberNumber) {
        mainTestTenant.setSubscriberNumber(subscriberNumber);
        tenantService.updateTenant(mainTestTenant);
    }

    private void register(String email, boolean expectSuccess) throws InterruptedException {
        int initialCount = getSeatCount();

        UserRegistration uReg = createUserReg(email);

        try {
            String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users/", uReg, String.class);
            ResponseDocument<RegistrationResult> response = ResponseDocument.
                    generateFromJSON(json, RegistrationResult.class);

            assertNotNull(response);
            assertEquals(response.isSuccess(), expectSuccess);
        } catch (RuntimeException e) {
            // allow expected 403 for user seat count limit
            if (expectSuccess)
                throw e;
            assertTrue(e.getMessage().startsWith(FORBIDDEN_MSG));
        }
        Thread.sleep(3000);
        int currCount = getSeatCount();

        if (expectSuccess && !EmailUtils.isInternalUser(email)) {
//            assertEquals(currCount, initialCount + 1);
            System.out.println( " status is false ");
            assertTrue( currCount > initialCount);
        } else {
            System.out.println( " status is true ");
            assertEquals(currCount, initialCount);
        }
    }

    private void delete(String email, boolean expectSuccess) throws InterruptedException {
        int initialCount = getSeatCount();
        String url = getRestAPIHostPort() + "/pls/users/\"" + email + "\"";
        SimpleBooleanResponse response = restTemplate.exchange(
                url, HttpMethod.DELETE, jsonRequestEntity(""), SimpleBooleanResponse.class).getBody();
        assertNotNull(response);
        assertEquals(response.isSuccess(), expectSuccess);

        Thread.sleep(3000);
        int currCount = getSeatCount();
        if (expectSuccess && !EmailUtils.isInternalUser(email)) {
            assertEquals(currCount, initialCount - 1);
        } else {
            assertEquals(currCount, initialCount);
        }
    }

    private void update(String email, boolean expectSuccess, AccessLevel level, boolean newUser) throws InterruptedException {
        int initialCount = getSeatCount();
        UserUpdateData data = new UserUpdateData();
        data.setAccessLevel(level.name());
        String url = getRestAPIHostPort() + "/pls/users/\"" + email + "\"";
        try {
            String json = restTemplate.exchange(url, HttpMethod.PUT, jsonRequestEntity(data), String.class).getBody();
            ResponseDocument<UserUpdateResponse> response = ResponseDocument.generateFromJSON(json, UserUpdateResponse.class);
            assertNotNull(response);
            assertEquals(response.isSuccess(), expectSuccess);
        } catch (RuntimeException e) {
            // allow expected 403 for user seat count limit
            if (expectSuccess)
                throw e;
            assertTrue(e.getMessage().startsWith(FORBIDDEN_MSG));
        }
        Thread.sleep(3000);
        int currCount = getSeatCount();
        if (expectSuccess && newUser && !EmailUtils.isInternalUser(email)) {
            assertEquals(currCount, initialCount + 1);
        } else {
            assertEquals(currCount, initialCount);
        }
    }

    private int getSeatCount() {
        JsonNode meter = vboService.getSubscriberMeter(mainTestTenant.getSubscriberNumber());
        assertNotNull(meter);
        assertTrue(meter.has("current_usage"));
        assertNotNull(meter.get("current_usage"));
        return meter.get("current_usage").asInt();
    }

    private void ensureSubscriberSeatState() throws InterruptedException {
        // ensure open
        int openSeats = getAvailableSeats(SUBSCRIBER_NUMBER_OPEN);

        while (openSeats < 1) {
            sendUsageEvent(VboUserSeatUsageEvent.FeatureURI.STDEC, SUBSCRIBER_NUMBER_OPEN);
            openSeats++;
        }
        // ensure full
        openSeats = getAvailableSeats(SUBSCRIBER_NUMBER_FULL);

        while (openSeats > 0) {
            sendUsageEvent(VboUserSeatUsageEvent.FeatureURI.STCT, SUBSCRIBER_NUMBER_FULL);
            openSeats--;
        }
        openSeats = getAvailableSeats(SUBSCRIBER_NUMBER_FULL);


        while (openSeats < 0) {
            sendUsageEvent(VboUserSeatUsageEvent.FeatureURI.STDEC, SUBSCRIBER_NUMBER_FULL);
            openSeats++;
        }
        openSeats = getAvailableSeats(SUBSCRIBER_NUMBER_FULL);

        Thread.sleep(3000);
        assertTrue(getAvailableSeats(SUBSCRIBER_NUMBER_OPEN) >= 1);
        assertTrue (getAvailableSeats(SUBSCRIBER_NUMBER_FULL) == 0);

    }

    private int getAvailableSeats(String subscriberNumber) {
        JsonNode meter = vboService.getSubscriberMeter(subscriberNumber);
        assertNotNull(meter);
        assertTrue(meter.has("current_usage"));
        assertTrue(meter.has("limit"));
        assertNotNull(meter.get("current_usage"));
        assertNotNull(meter.get("limit"));
        int seat = meter.get("limit").asInt() - meter.get("current_usage").asInt();

        return meter.get("limit").asInt() - meter.get("current_usage").asInt();
    }

    private void sendUsageEvent(VboUserSeatUsageEvent.FeatureURI feature, String subscriberNumber) {
        VboUserSeatUsageEvent usageEvent = new VboUserSeatUsageEvent();
        Date current_date = new Date();
        usageEvent.setSubscriberID(subscriberNumber);
        usageEvent.setFeatureURI(feature);
        usageEvent.setPOAEID("99");
        usageEvent.setLUID(null);
        usageEvent.setTimeStamp(ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT));
        usageEvent.setSubjectCountry("US");
        usageEvent.setSubscriberCountry("US");
        usageEvent.setContractTermStartDate(current_date);
        usageEvent.setContractTermEndDate(DateUtils.addHours(current_date, 1));
        vboService.sendUserUsageEvent(usageEvent);
    }

    private UserRegistration createUserReg(String email) {
        UserRegistration userReg = new UserRegistration();

        User user = new User();
        user.setEmail(email);
        user.setFirstName("Lattice");
        user.setLastName("Tester");
        user.setPhoneNumber("650-555-5555");
        user.setTitle("Silly Tester");
        user.setAccessLevel(AccessLevel.EXTERNAL_USER.name());

        Credentials creds = new Credentials();
        creds.setUsername(user.getEmail());
        creds.setPassword("WillBeModifiedImmediately");

        user.setUsername(creds.getUsername());

        userReg.setUser(user);
        userReg.setCredentials(creds);

        return userReg;
    }

}
