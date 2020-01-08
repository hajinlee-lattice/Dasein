package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthTicket;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalSessionManagementService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

/**
 * Simulate login at the service level which authenticates then attaches.
 *
 * @author rgonzalez
 */
public class GlobalSessionManagementServiceImplTestNG extends SecurityFunctionalTestNGBase {

    private Ticket ticket;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementService globalSessionManagementService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private GlobalAuthTenantEntityMgr globalAuthTenantEntityMgr;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        String passwd = DigestUtils.sha256Hex(adminPassword);
        try {
            ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        } catch (Exception e) {
            createAdminUser();
            ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        }
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
        GlobalAuthTenant tenant = globalAuthTenantEntityMgr.findByTenantId(adminTenantName);
        assertNotNull(tenant);
        Long userId = globalUserManagementService.getIdByUsername(adminUsername);
        List<GlobalAuthTicket> tickets = globalSessionManagementService.findTicketsByUserIdAndTenant(userId, tenant);
        assertTrue(tickets.size() > 0);
        assertEquals(tickets.get(0).getTicket(), ticket.getData());
    }

    @Test(groups = "functional", dependsOnMethods = {"attach"})
    public void retrieve() {
        Ticket t = new Ticket(ticket.getUniqueness() + "." + ticket.getRandomness());
        Session session = globalSessionManagementService.retrieve(t);
        assertNotNull(session);
        assertNotNull(session.getTicket());
        assertNotNull(session.getTenant());
    }

}
