package com.latticeengines.pls.globalauth.authentication.impl;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalSessionManagementService;

/**
 * Simulate login at the service level which authenticates then attaches.
 * @author rgonzalez
 *
 */
public class GlobalSessionManagementServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    private Ticket ticket;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        String passwd = DigestUtils.sha256Hex(adminPassword);
        ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        assertNotNull(ticket);
    }
    
    @Test(groups = "functional", expectedExceptions = NullPointerException.class)
    public void attachNullTicket() {
        globalSessionManagementService.attach(null);
    }

    @Test(groups = "functional")
    public void attach() {
        Session session = globalSessionManagementService.attach(ticket);
        assertNotNull(session);
    }

    @Test(groups = "functional", dependsOnMethods = { "attach" })
    public void retrieve() {
        Ticket t = new Ticket(ticket.getUniqueness() + "." + ticket.getRandomness());
        Session session = globalSessionManagementService.retrieve(t);
        assertNotNull(session);
        assertTrue(session.getRights().size() >=4);
        assertNotNull(session.getTicket());
        assertNotNull(session.getTenant());
    }

}
