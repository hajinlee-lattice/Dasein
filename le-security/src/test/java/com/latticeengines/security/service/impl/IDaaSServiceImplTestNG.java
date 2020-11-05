package com.latticeengines.security.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.dcp.idaas.InvitationLinkResponse;
import com.latticeengines.domain.exposed.dcp.idaas.SubscriberDetails;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.LoginTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.security.exposed.service.UserService;
import com.latticeengines.security.service.IDaaSService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-security-context.xml" })
public class IDaaSServiceImplTestNG extends AbstractTestNGSpringContextTests {

    private static final String TEST_EMAIL = "dcp_dev@lattice-engines.com";
    private static final String TEST_PASSWORD = "Lattice124!";

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
        credentials.setPassword(TEST_PASSWORD);
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

    @Test(groups = "deployment")
    public void testLoginDoc() {
        Credentials credentials = new Credentials();
        credentials.setUsername(TEST_EMAIL);
        credentials.setPassword(TEST_PASSWORD);
        LoginDocument loginDocument = iDaaSService.login(credentials);
        Assert.assertNotNull(loginDocument);
        LoginDocument.LoginResult result = loginDocument.getResult();
        Assert.assertNotNull(result);
        List<Tenant> tenantList = result.getTenants();
        Assert.assertNotNull(tenantList);
        Assert.assertFalse(tenantList.isEmpty());
        Tenant firstTenant = tenantList.get(0);
        Assert.assertTrue( firstTenant instanceof LoginTenant );
        Map<String,LoginTenant> tenantMap = new HashMap<>(tenantList.size());
        for (Tenant t : tenantList) {
            tenantMap.put(t.getId(), (LoginTenant) t);
        }
        LoginTenant lt = tenantMap.get("LETest1603910712497.LETest1603910712497.Production");
        Assert.assertNotNull(lt);
        Assert.assertEquals(lt.getDuns(), "202007226");
        Assert.assertEquals(lt.getCompanyName(), "D&B Connect Engineering");
        Assert.assertEquals(lt.getStatus(), TenantStatus.ACTIVE);
    }

    @Test(groups = "functional")
    public void testGetUserInvitationLink() {
        InvitationLinkResponse invitationLink = iDaaSService.getUserInvitationLink(TEST_EMAIL);
        Assert.assertNotNull(invitationLink);
        Assert.assertNotNull(invitationLink.getInviteLink());
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

    @Test(groups = "deployment")
    public void testGetSubscriberDetails () {

        String subscriptionNumber = "800118741";
        SubscriberDetails subscriberDetails = iDaaSService.getSubscriberDetails(subscriptionNumber);
        Assert.assertNotNull(subscriberDetails);
        Assert.assertEquals("167734092", subscriberDetails.getDunsNumber(), "DUNS number not equal.");
    }

}
