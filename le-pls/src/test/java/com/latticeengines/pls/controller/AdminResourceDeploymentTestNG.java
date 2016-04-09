package com.latticeengines.pls.controller;

import java.util.Arrays;

import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.functionalframework.PlsDeploymentTestNGBaseDeprecated;
import com.latticeengines.security.exposed.Constants;

public class AdminResourceDeploymentTestNG extends PlsDeploymentTestNGBaseDeprecated {

    private static final String USER_EMAIL = "ron@lattice-engines.com";

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        switchToSuperAdmin();
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

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        Boolean result = magicRestTemplate.postForObject(getDeployedRestAPIHostPort() + "/pls/admin/users",
                userRegistrationWithTenant, Boolean.class);
        Assert.assertTrue(result);

        UserDocument userDoc = loginAndAttach(USER_EMAIL, generalPassword, mainTestTenant);
        Assert.assertNotNull(userDoc);
    }

}
