package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.cdl.SubscriptionProxy;

public class SubscriptionResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionResourceDeploymentTestNG.class);

    @Inject
    private SubscriptionProxy subscriptionProxy;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void testGet() {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        Assert.assertEquals(0, getEmailsCount(tenantId));
        Set<String> validEmails = initEmailSet(new String[] { "ga_dev@lattice-engines.com" });
        subscriptionProxy.saveByEmailsAndTenantId(validEmails, tenantId);
        Assert.assertEquals(validEmails.size(), getEmailsCount(tenantId));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGet" })
    public void testCreate() {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String[] validEmailArray = { "pls-super-admin-tester@lattice-engines.com", "ysong@lattice-engines.com",
                "bross@lattice-engines.com" };
        Set<String> validEmails = initEmailSet(validEmailArray);
        int expectedCount = getEmailsCount(tenantId);

        List<String> savedEmails = subscriptionProxy.saveByEmailsAndTenantId(validEmails, tenantId);
        Assert.assertTrue(verityEmailsSaved(validEmails, tenantId));
        expectedCount += savedEmails.size();

        Set<String> inValidEmails = initEmailSet(new String[] { "invalid@lattice-engines.com" });
        savedEmails = subscriptionProxy.saveByEmailsAndTenantId(inValidEmails, tenantId);
        Assert.assertEquals(0, savedEmails.size());
        Assert.assertEquals(expectedCount, getEmailsCount(tenantId));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreate" })
    public void testDelete() {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        List<String> emails = subscriptionProxy.getEmailsByTenantId(tenantId);
        Assert.assertTrue(CollectionUtils.isNotEmpty(emails));
        for (String email : emails) {
            subscriptionProxy.deleteByEmailAndTenantId(email, tenantId);
        }
        Assert.assertEquals(0, getEmailsCount(tenantId));
    }

    private int getEmailsCount(String tenantId) {
        List<String> emails = subscriptionProxy.getEmailsByTenantId(tenantId);
        return CollectionUtils.isEmpty(emails) ? 0 : emails.size();
    }

    private boolean verityEmailsSaved(Set<String> inputEmails, String tenantId) {
        Set<String> subscriptEmails = new HashSet<>(subscriptionProxy.getEmailsByTenantId(tenantId));
        for (String email : inputEmails) {
            if (!subscriptEmails.contains(email))
                return false;
        }
        return true;
    }

    private Set<String> initEmailSet(String[] array) {
        return new HashSet<>(Arrays.asList(array));
    }

}
