package com.latticeengines.pls.globalauth.authentication.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;

public class GlobalAuthenticationServiceImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Test(groups = "functional")
    public void authenticateThenDiscardUser() {
        String passwd = DigestUtils.sha256Hex("admin");
        Ticket ticket = globalAuthenticationService.authenticateUser("admin", passwd);
        assertNotNull(ticket);
        assertEquals(ticket.getTenants().size(), 2);

        boolean result = globalAuthenticationService.discard(ticket);
        assertTrue(result);
    }

    @Test(groups = "functional")
    public void authenticateUserWrongPassword() {
        String passwd = DigestUtils.sha256Hex("admin1");
        try {
            globalAuthenticationService.authenticateUser("admin", passwd);
        } catch (Exception e) {
            assertTrue(e instanceof LedpException);
            assertEquals(((LedpException) e).getCode(), LedpCode.LEDP_18001);
        }

    }
}
