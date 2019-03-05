package com.latticeengines.admin.service.impl;

import static org.testng.Assert.assertTrue;

import java.util.Date;

import javax.inject.Inject;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.ProspectingUserService;
import com.latticeengines.domain.exposed.pls.RegistrationResult;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.service.UserService;

public class ProspectingUserServiceTestNG extends AdminFunctionalTestNGBase {

    @Inject
    private ProspectingUserService pUserService;

    @Inject
    private UserService userService;

    @BeforeClass(groups = { "functional" })
    public void setup() {
    }

    @AfterClass(groups = { "functional" })
    public void tearDown() {
    }

    @Test(groups = "functional")
    public void createUser() {
        Date now = new Date();
        Long longTime = now.getTime() / 1000L;
        String current_time_s = longTime.toString();
        UserRegistration userReg = new UserRegistration();
        Credentials cred = new Credentials();
        User user = new User();
        String email = "PUE" + current_time_s + "@lattice-engines.com";
        user.setEmail(email);
        user.setUsername("PU" + current_time_s);
        user.setFirstName("F" + current_time_s);
        user.setLastName("L" + current_time_s);
        cred.setUsername("PU" + current_time_s);
        cred.setPassword("xxx");
        userReg.setCredentials(cred);
        userReg.setUser(user);
        RegistrationResult result = pUserService.createUser(userReg);
        System.out.println(result);
        assertTrue(result.isValid());
        userService.deleteUserByEmail(email);
    }
}
