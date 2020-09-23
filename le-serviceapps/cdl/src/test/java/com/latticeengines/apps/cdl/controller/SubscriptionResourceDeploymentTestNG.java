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
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        Assert.assertEquals(0, getEmailsCount(customerSpace));

        Set<String> validEmails = initEmailSet(new String[] { "ga_dev@lattice-engines.com" });
        subscriptionProxy.saveByEmailsAndTenantId(validEmails, customerSpace.getTenantId(),
                customerSpace.getContractId());
        Assert.assertEquals(1, getEmailsCount(customerSpace));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGet" })
    public void testCreate() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String[] validEmailArray = { "pls-super-admin-tester@lattice-engines.com", "ysong@lattice-engines.com",
                "bross@lattice-engines.com" };
        Set<String> validEmails = initEmailSet(validEmailArray);
        int expectedCount = getEmailsCount(customerSpace);

        List<String> savedEmails = subscriptionProxy.saveByEmailsAndTenantId(validEmails, customerSpace.getTenantId(),
                customerSpace.getContractId());
        Assert.assertEquals(savedEmails.size(), validEmailArray.length);
        expectedCount += savedEmails.size();
        Assert.assertEquals(expectedCount, getEmailsCount(customerSpace));

        Set<String> inValidEmails = initEmailSet(new String[] { "invalid@lattice-engines.com" });
        savedEmails = subscriptionProxy.saveByEmailsAndTenantId(inValidEmails, customerSpace.getTenantId(),
                customerSpace.getContractId());
        Assert.assertEquals(0, savedEmails.size());
        Assert.assertEquals(expectedCount, getEmailsCount(customerSpace));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreate" })
    public void testDelete() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<String> emails = subscriptionProxy.getEmailsByTenantId(customerSpace.getTenantId(),
                customerSpace.getContractId());
        Assert.assertTrue(CollectionUtils.isNotEmpty(emails));
        for (String email : emails) {
            subscriptionProxy.deleteByEmailAndTenantId(email, customerSpace.getTenantId(),
                    customerSpace.getContractId());
        }
        Assert.assertEquals(0, getEmailsCount(customerSpace));
    }

    private int getEmailsCount(CustomerSpace customerSpace) {
        List<String> emails = subscriptionProxy.getEmailsByTenantId(customerSpace.getTenantId(),
                customerSpace.getContractId());
        return CollectionUtils.isEmpty(emails) ? 0 : emails.size();
    }

    private Set<String> initEmailSet(String[] array) {
        return new HashSet<>(Arrays.asList(array));
    }

}
