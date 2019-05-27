package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalAuthenticationServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        try {
            login(adminUsername, adminPassword);
        } catch (Exception e) {
            createAdminUser();
        }
    }

    @Test(groups = "functional")
    public void authenticateThenDiscardUser() {
        String passwd = DigestUtils.sha256Hex(adminPassword);
        Ticket ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        assertNotNull(ticket);
        assertTrue(ticket.getTenants().size() >= 1);

        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
    }

    @Test(groups = "functional")
    public void authenticateUserWrongPassword() {
        String passwd = DigestUtils.sha256Hex("wrong");
        boolean expectEx = false;
        try {
            globalAuthenticationService.authenticateUser(adminUsername, passwd);
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18001);
            expectEx = true;
        }
        Assert.assertTrue(expectEx);
        //reset invalid count;
        globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
        for (int i = 0; i < 5; i++) {
            try {
                globalAuthenticationService.authenticateUser(adminUsername, passwd);
            } catch (Exception e) {
                assertTrue(e instanceof LedpException);
                if (i < 4) {
                    assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18001);
                } else {
                    assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_19015);
                }
            }
        }
        //try correct password
        expectEx = false;
        try {
            globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_19015);
            expectEx = true;
        }
        Assert.assertTrue(expectEx);

        //reset password
        String tempPass = globalUserManagementService.resetLatticeCredentials(adminUsername);
        Ticket ticket = globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(tempPass));
        Assert.assertNotNull(ticket);


        //update to original password
        Credentials oldCreds = new Credentials();
        oldCreds.setUsername(adminUsername);
        oldCreds.setPassword(DigestUtils.sha256Hex(tempPass));
        Credentials newCreds = new Credentials();
        newCreds.setUsername(adminUsername);
        newCreds.setPassword(DigestUtils.sha256Hex(adminPassword));
        Assert.assertTrue(globalUserManagementService.modifyLatticeCredentials(ticket, oldCreds, newCreds));

        // try login again
        Assert.assertNotNull(globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword)));

    }
}
