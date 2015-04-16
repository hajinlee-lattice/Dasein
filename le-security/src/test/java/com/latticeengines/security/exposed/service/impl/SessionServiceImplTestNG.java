package com.latticeengines.security.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

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
        ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        assertNotNull(ticket);
        Session session = loginAndAttach(adminUsername, adminPassword);
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
        Ticket t = new Ticket(ticket.getUniqueness() + "." + ticket.getRandomness());
        Session session = sessionService.retrieve(t);
        assertNotNull(session);
        assertTrue(session.getRights().size() >=4);
        assertNotNull(session.getTicket());
        assertNotNull(session.getTenant());
    }

    @Test(groups = "functional", dependsOnMethods = { "attach" })
    public void decodeGlobalAuthRights() {
        testLevelInLevelOut();
        testLevelRightsInLevelOut();
    }

    private void testLevelInLevelOut() {
        makeSureUserDoesNotExist(testUsername);
        createUser(testUsername, testUsername, "Test", "Tester", generalPasswordHash);

        grantRight(AccessLevel.INTERNAL_ADMIN.name(), tenant.getId(), testUsername);

        Session session = loginAndAttach(testUsername);

        assertEquals(session.getAccessLevel(), AccessLevel.INTERNAL_ADMIN.name());
        Set<String> rightsInGA = new HashSet<>(session.getRights());
        for (GrantedRight right: AccessLevel.INTERNAL_ADMIN.getGrantedRights()) {
            rightsInGA.remove(right.getAuthority());
        }
        assertTrue(rightsInGA.isEmpty());

        makeSureUserDoesNotExist(testUsername);
    }

    private void testLevelRightsInLevelOut() {
        makeSureUserDoesNotExist(testUsername);
        createUser(testUsername, testUsername, "SessionService", "Tester", generalPasswordHash);

        for (GrantedRight right : AccessLevel.SUPER_ADMIN.getGrantedRights()) {
            grantRight(right.getAuthority(), tenant.getId(), testUsername);
        }

        Session session = loginAndAttach(testUsername);

        assertNull(session.getAccessLevel());
        Set<String> rightsInGA = new HashSet<>(session.getRights());
        for (GrantedRight right: AccessLevel.SUPER_ADMIN.getGrantedRights()) {
            rightsInGA.remove(right.getAuthority());
        }
        assertTrue(rightsInGA.isEmpty());

        makeSureUserDoesNotExist(testUsername);
    }

}
