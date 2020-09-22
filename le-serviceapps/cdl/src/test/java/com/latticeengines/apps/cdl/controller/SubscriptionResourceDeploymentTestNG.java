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
import com.latticeengines.auth.exposed.service.GlobalAuthSubscriptionService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.proxy.exposed.cdl.SubscriptionProxy;

public class SubscriptionResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ActionResourceDeploymentTestNG.class);

    @Inject
    private SubscriptionProxy subscriptionProxy;

    @Inject
    private GlobalAuthSubscriptionService subscriptionService;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void testGet() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<String> emails = getEmails(customerSpace);
        Assert.assertEquals(emails.size(), 0);

        Set<String> validEmails = initEmailSet(new String[] { "ga_dev@lattice-engines.com" });
        subscriptionProxy.saveByEmailsAndTenantId(validEmails, customerSpace.getTenantId(),
                customerSpace.getContractId());
        emails = getEmails(customerSpace);
        Assert.assertEquals(emails.size(), 1);
    }

    @Test(groups = "deployment-app")
    public void testCreate() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        String[] validEmailArray = { "pls-super-admin-tester@lattice-engines.com", "ysong@lattice-engines.com",
                "bross@lattice-engines.com" };
        List<String> emails = getEmails(customerSpace);
        int count = emails == null ? 0 : emails.size();
        Set<String> validEmails = initEmailSet(validEmailArray);
        int savedCount = subscriptionProxy
                .saveByEmailsAndTenantId(validEmails, customerSpace.getTenantId(), customerSpace.getSpaceId()).size();
        Assert.assertEquals(savedCount, 3);
        emails = getEmails(customerSpace);
        count += savedCount;
        Assert.assertEquals(emails.size(), count);

        Set<String> inValidEmailList = initEmailSet(new String[] { "invalid@lattice-engines.com" });
        savedCount = subscriptionProxy
                .saveByEmailsAndTenantId(inValidEmailList, customerSpace.getTenantId(), customerSpace.getSpaceId())
                .size();
        Assert.assertEquals(savedCount, 0);
        emails = getEmails(customerSpace);
        Assert.assertEquals(emails.size(), count);
    }

    @Test(groups = "deployment-app", dependsOnMethods = "testCreate")
    public void testDelete() {
        CustomerSpace customerSpace = CustomerSpace.parse(mainTestTenant.getId());
        List<String> emails = getEmails(customerSpace);
        Assert.assertTrue(CollectionUtils.isNotEmpty(emails));
        for (String email : emails) {
            subscriptionProxy.deleteByEmailAndTenantId(email, customerSpace.getTenantId(), customerSpace.getSpaceId());
        }
        emails = getEmails(customerSpace);
        Assert.assertTrue(CollectionUtils.isEmpty(emails));
    }

    private List<String> getEmails(CustomerSpace customerSpace) {
        return subscriptionProxy.getEmailsByTenantId(customerSpace.getTenantId(), customerSpace.getContractId());
    }

    private Set<String> initEmailSet(String[] array) {
        return new HashSet<>(Arrays.asList(array));
    }

}
