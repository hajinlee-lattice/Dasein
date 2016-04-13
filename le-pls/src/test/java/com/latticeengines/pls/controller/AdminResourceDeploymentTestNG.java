package com.latticeengines.pls.controller;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBase;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class AdminResourceDeploymentTestNG extends PlsDeploymentTestNGBase {

    private static final String USER_EMAIL = "ron@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        setupTestEnvironmentWithOneTenant();
        deleteUserByRestCall(USER_EMAIL);
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() {
        deleteUserByRestCall(USER_EMAIL);
    }

    @Test(groups = "deployment")
    public void addAdminUser() {
        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        userRegistrationWithTenant.setTenant(mainTestTenant.getId());
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        userRegistration.setUser(AdminResourceTestNG.getUser());
        userRegistration.setCredentials(AdminResourceTestNG.getCreds());

        Boolean result = magicRestTemplate.postForObject(getDeployedRestAPIHostPort() + "/pls/admin/users",
                userRegistrationWithTenant, Boolean.class);
        Assert.assertTrue(result);

        UserDocument userDoc = testBed.loginAndAttach(USER_EMAIL, TestFrameworkUtils.GENERAL_PASSWORD, mainTestTenant);
        Assert.assertNotNull(userDoc);
    }

}
