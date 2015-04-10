package com.latticeengines.security.exposed.globalauth.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalAuthenticationServiceImplTestNG extends SecurityFunctionalTestNGBase {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Test(groups = "functional")
    public void authenticateThenDiscardUser() {
        String passwd = DigestUtils.sha256Hex(adminPassword);
        Ticket ticket = globalAuthenticationService.authenticateUser(adminUsername, passwd);
        assertNotNull(ticket);
        assertEquals(ticket.getTenants().size(), 2);

        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
    }

    @Test(groups = "functional")
    public void authenticateUserWrongPassword() {
        String passwd = DigestUtils.sha256Hex("wrong");
        try {
            globalAuthenticationService.authenticateUser(adminUsername, passwd);
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18001);
        }

    }
}
