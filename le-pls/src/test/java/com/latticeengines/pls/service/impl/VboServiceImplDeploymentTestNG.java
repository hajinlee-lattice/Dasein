package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpMethod;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.latticeengines.common.exposed.util.EmailUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
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
 * $ dpltc deploy -a admin,matchapi,microservice,pls -m metadata,lp
 */

public class VboServiceImplDeploymentTestNG extends PlsDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(VboServiceImplDeploymentTestNG.class);

    private static final String SUBSCRIBER_NUMBER_OPEN = "500118856";
    private static final String SUBSCRIBER_NUMBER_FULL = "500118852";
    private static final String INTERNAL_USER_EMAIL = "build@lattice-engines.com";
    private static final String EXTERNAL_USER_EMAIL = "build.lattice.engines@gmail.com";

    private static final String FORBIDDEN_MSG = "403 FORBIDDEN";

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
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
    }

    @AfterClass(groups = { "deployment" })
    public void tearDown() {
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
    }

    @Test(groups = "deployment")
    public void testRegisterInternalEmail() throws InterruptedException {
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        String[] subscriberNumbers = {SUBSCRIBER_NUMBER_OPEN, SUBSCRIBER_NUMBER_FULL};
        for (String sn : subscriberNumbers) {
            switchSubscriber(sn);

            // register internal user: assert success; no meter change
            register(INTERNAL_USER_EMAIL, true);

            // delete internal user: assert success; no meter change
            delete(INTERNAL_USER_EMAIL, true);
        }
    }

    @Test(groups = "deployment")
    public void testRegisterExternalEmail() throws InterruptedException {
        // subscriber with open seats: assert success, meter change
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
        switchSubscriber(SUBSCRIBER_NUMBER_OPEN);
        register(EXTERNAL_USER_EMAIL, true);
        delete(EXTERNAL_USER_EMAIL, true);

        // subscriber at limit: assert fail, no meter change
        switchSubscriber(SUBSCRIBER_NUMBER_FULL);
        register(EXTERNAL_USER_EMAIL, false);
        delete(EXTERNAL_USER_EMAIL, false);
    }

    @Test(groups = "deployment")
    public void testUpdateInternalEmail() throws InterruptedException {
        deleteUserByRestCall(INTERNAL_USER_EMAIL);
        String[] subscriberNumbers = {SUBSCRIBER_NUMBER_OPEN, SUBSCRIBER_NUMBER_FULL};
        for (String sn : subscriberNumbers) {
            switchSubscriber(sn);

            // update new internal user: assert success, meter change
            userService.createUser(INTERNAL_USER_EMAIL, getUserReg(INTERNAL_USER_EMAIL));
            update(INTERNAL_USER_EMAIL, true, AccessLevel.EXTERNAL_USER, true);

            delete(INTERNAL_USER_EMAIL, true);
        }
    }

    @Test(groups = "deployment")
    public void testUpdateExternalEmail() throws InterruptedException {
        // subscriber with open seats, new external user: assert success, meter change
        deleteUserByRestCall(EXTERNAL_USER_EMAIL);
        switchSubscriber(SUBSCRIBER_NUMBER_OPEN);
        userService.createUser(EXTERNAL_USER_EMAIL, getUserReg(EXTERNAL_USER_EMAIL));
        update(EXTERNAL_USER_EMAIL, true, AccessLevel.EXTERNAL_USER, true);

        // existing external user: assert success, no meter change
        update(EXTERNAL_USER_EMAIL, true, AccessLevel.EXTERNAL_ADMIN, false);
        delete(EXTERNAL_USER_EMAIL, true);

        // subscriber at limit, new external user: assert 403, seat count unchanged
        switchSubscriber(SUBSCRIBER_NUMBER_FULL);
        userService.createUser(EXTERNAL_USER_EMAIL, getUserReg(EXTERNAL_USER_EMAIL));
        update(EXTERNAL_USER_EMAIL, false, AccessLevel.EXTERNAL_USER, true);
        delete(EXTERNAL_USER_EMAIL, false);
        userService.deleteUserByEmail(EXTERNAL_USER_EMAIL);

        // TODO: external user and seats full but user is already existing: expect successful update, no meter change
    }

    private void switchSubscriber(String subscriberNumber) {
        mainTestTenant.setSubscriberNumber(subscriberNumber);
        tenantService.updateTenant(mainTestTenant);
    }

    private void register(String email, boolean expectSuccess) throws InterruptedException {
        int initialCount = getSeatCount();
        UserRegistration uReg = getUserReg(email);
        try {
            String json = restTemplate.postForObject(getRestAPIHostPort() + "/pls/users/", uReg, String.class);
            ResponseDocument<RegistrationResult> response = ResponseDocument.
                    generateFromJSON(json, RegistrationResult.class);
            assertNotNull(response);
            log.error(response.toString());
            assertEquals(response.isSuccess(), expectSuccess);
        } catch (RuntimeException e) {
            // allow expected 403 for user seat count limit
            if (expectSuccess)
                throw e;
            assertEquals(e.getMessage(), FORBIDDEN_MSG);
        }
        Thread.sleep(3000);
        int currCount = getSeatCount();
        if (expectSuccess && !EmailUtils.isInternalUser(email)) {
            assertEquals(currCount, initialCount + 1);
        } else {
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
            assertEquals(e.getMessage(), FORBIDDEN_MSG);
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
        return meter.get("current_usage").asInt();
    }

    private UserRegistration getUserReg(String email) {
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
