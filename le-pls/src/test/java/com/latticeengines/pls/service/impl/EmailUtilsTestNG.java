package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

import junit.framework.Assert;

public class EmailUtilsTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private EmailUtils emailUtils;

    @Value("${pls.test.internalemail}")
    private boolean testInternalEmail;

    @Value("${pls.test.externalemail}")
    private boolean testExternalEmail;

    @Test(groups = "deployment")
    public void testSendEmails() throws Exception {
        Tenant tenant = new Tenant();
        tenant.setId("Email_Tenant");
        tenant.setName("Tenant for email testing");

        User user = new User();
        user.setEmail("ysong@lattice-engines.com");
        user.setUsername(user.getEmail());
        user.setFirstName("Yintao");
        user.setLastName("Song");
        String password = "temp_password";

        if (testInternalEmail) {
            Assert.assertTrue(emailUtils.sendNewInternalUserEmail(tenant, user, password));
            Assert.assertTrue(emailUtils.sendExistingInternalUserEmail(tenant, user));
        }

        user.setEmail("yintaosong@gmail.com");
        user.setUsername(user.getEmail());
        if (testExternalEmail) {
            Assert.assertTrue(emailUtils.sendNewExternalUserEmail(user, password));
        }
    }
}
