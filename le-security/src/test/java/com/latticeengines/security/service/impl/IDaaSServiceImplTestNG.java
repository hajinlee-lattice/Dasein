package com.latticeengines.security.service.impl;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.IDaaSService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-security-context.xml" })
public class IDaaSServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String TEST_EMAIL = "dcp_dev@lattice-engines.com";

    @Inject
    private IDaaSService iDaaSService;

    @Inject
    private UserService userService;

    @BeforeClass(groups = "functional")
    public void setup() {
        User user = userService.findByUsername(TEST_EMAIL);
        if (user == null) {
            createTestUser();
        }
    }

    @Test(groups = "functional")
    public void testLogin() {
        Credentials credentials = new Credentials();
        credentials.setUsername(TEST_EMAIL);
        credentials.setPassword("Lattice123!");
        LoginDocument loginDocument = iDaaSService.login(credentials);
        Assert.assertNotNull(loginDocument);
        Assert.assertTrue(CollectionUtils.isEmpty(loginDocument.getErrors()));
        Assert.assertNotNull(loginDocument.getUniqueness());
        Assert.assertNotNull(loginDocument.getRandomness());

        credentials.setUsername("wrong@gmail.com");
        loginDocument = iDaaSService.login(credentials);
        Assert.assertNotNull(loginDocument);
        Assert.assertTrue(CollectionUtils.isNotEmpty(loginDocument.getErrors()));
    }

    private void createTestUser() {
        User user = new User();
        user.setEmail(TEST_EMAIL);
        user.setUsername(TEST_EMAIL);

        Credentials credentials = new Credentials();
        credentials.setUsername(TEST_EMAIL);
        credentials.setPassword("Lattice123!");

        UserRegistration userRegistration = new UserRegistration();
        userRegistration.setUser(user);
        userRegistration.setCredentials(credentials);

        userService.createUser(TEST_EMAIL, userRegistration);
    }

}
