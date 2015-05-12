package com.latticeengines.pls.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

import junit.framework.Assert;

public class EmailUtilsTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private EmailUtils emailUtils;

    @Test(groups = {"functional", "deployment"})
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

        Assert.assertTrue(emailUtils.sendNewInternalUserEmail(tenant, user, password));
        Assert.assertTrue(emailUtils.sendExistingInternalUserEmail(tenant, user));

        user.setEmail("yintaosong@gmail.com");
        user.setUsername(user.getEmail());
        Assert.assertTrue(emailUtils.sendNewExternalUserEmail(user, password));
    }
}
