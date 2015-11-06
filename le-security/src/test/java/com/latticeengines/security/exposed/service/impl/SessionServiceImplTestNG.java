package com.latticeengines.security.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.GrantedRight;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.service.SessionService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class SessionServiceImplTestNG extends SecurityFunctionalTestNGBase {

    private Ticket ticket;
    private Tenant tenant;
    private final String testUsername = "sessionservice_tester@test.lattice-engines.com";

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private SessionService sessionService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        String passwd = DigestUtils.sha256Hex(adminPassword);
        ticket = globalAuthenticationService.authenticateUser(adminUsername,
                passwd);
        assertNotNull(ticket);
        Session session = login(adminUsername, adminPassword);
        tenant = session.getTenant();
    }

    @Test(groups = "functional", expectedExceptions = NullPointerException.class)
    public void attachNullTicket() {
        sessionService.attach(null);
    }

    @Test(groups = "functional")
    public void attach() {
        Session session = sessionService.attach(ticket);
        assertNotNull(session);
    }

    @Test(groups = "functional", dependsOnMethods = { "attach" })
    public void retrieve() {
        Ticket t = new Ticket(ticket.getUniqueness() + "."
                + ticket.getRandomness());
        Session session = sessionService.retrieve(t);
        assertNotNull(session);
        assertTrue(session.getRights().size() >= 4);
        assertNotNull(session.getTicket());
        assertNotNull(session.getTenant());
    }

    @Test(groups = "functional", dependsOnMethods = { "attach" })
    public void interpretGlobalAuthRights() {
        // rights in rights out
        List<GrantedRight> rightsIn = Arrays.asList(
                GrantedRight.VIEW_PLS_CONFIGURATIONS,
                GrantedRight.VIEW_PLS_MODELS);
        AccessLevel levelIn;
        List<GrantedRight> rightsOut = AccessLevel.INTERNAL_USER
                .getGrantedRights();
        AccessLevel levelOut = AccessLevel.INTERNAL_USER;
        testInterpretGARights(rightsIn, null, rightsOut, levelOut);

        rightsIn = Arrays.asList(GrantedRight.VIEW_PLS_CONFIGURATIONS,
                GrantedRight.VIEW_PLS_MODELS, GrantedRight.VIEW_PLS_MODELS,
                GrantedRight.EDIT_PLS_MODELS);
        rightsOut = AccessLevel.INTERNAL_USER.getGrantedRights();
        levelOut = AccessLevel.INTERNAL_USER;
        testInterpretGARights(rightsIn, null, rightsOut, levelOut);

        // level in level out
        rightsIn = new ArrayList<>();
        levelIn = AccessLevel.SUPER_ADMIN;
        rightsOut = AccessLevel.SUPER_ADMIN.getGrantedRights();
        levelOut = AccessLevel.SUPER_ADMIN;
        testInterpretGARights(rightsIn, levelIn, rightsOut, levelOut);

        // level + rights in level + rights out
        rightsIn = Arrays.asList(GrantedRight.VIEW_PLS_MODELS,
                GrantedRight.VIEW_PLS_CONFIGURATIONS);
        levelIn = AccessLevel.INTERNAL_USER;
        rightsOut = AccessLevel.INTERNAL_USER.getGrantedRights();
        levelOut = AccessLevel.INTERNAL_USER;
        testInterpretGARights(rightsIn, levelIn, rightsOut, levelOut);
    }

    private void testInterpretGARights(List<GrantedRight> rightsIn,
            AccessLevel levelIn, List<GrantedRight> rightsOut,
            AccessLevel levelOut) {
        makeSureUserDoesNotExist(testUsername);
        createUser(testUsername, testUsername, "Test", "Tester",
                generalPasswordHash);

        if (levelIn != null) {
            grantRight(levelIn.name(), tenant.getId(), testUsername);
        }
        for (GrantedRight right : rightsIn) {
            grantRight(right.getAuthority(), tenant.getId(), testUsername);
        }

        Session session = login(testUsername);

        if (levelOut == null) {
            assertNull(session.getAccessLevel());
        } else {
            assertEquals(session.getAccessLevel(), levelOut.name());
        }

        List<String> rightsInSession = new ArrayList<>();
        rightsInSession.addAll(session.getRights());

        rightsInSession.removeAll(GrantedRight.getAuthorities(rightsOut));
        assertTrue(rightsInSession.isEmpty());

        List<String> valuesOut = GrantedRight.getAuthorities(rightsOut);
        valuesOut.removeAll(session.getRights());
        assertTrue(valuesOut.isEmpty());

        makeSureUserDoesNotExist(testUsername);
    }

}
