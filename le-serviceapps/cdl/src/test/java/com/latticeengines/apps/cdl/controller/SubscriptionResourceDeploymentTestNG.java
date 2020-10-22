package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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

    private String emailKey = "emails";

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        setupTestEnvironment();
    }

    @Test(groups = "deployment-app")
    public void testGet() {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        Assert.assertEquals(0, getEmailsCount(tenantId));
        Map<String, Set<String>> validEmails = initEmailSet(new String[] { "ga_dev@lattice-engines.com" });
        subscriptionProxy.saveByEmailsAndTenantId(validEmails, tenantId);
        Assert.assertEquals(validEmails.size(), getEmailsCount(tenantId));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGet" })
    public void testCreate() {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        String[] validEmailArray = { "pls-super-admin-tester@lattice-engines.com", "ysong@lattice-engines.com",
                "bross@lattice-engines.com" };
        Map<String, Set<String>> validEmails = initEmailSet(validEmailArray);
        int expectedCount = getEmailsCount(tenantId);

        Map<String, List<String>> savedEmails = subscriptionProxy.saveByEmailsAndTenantId(validEmails, tenantId);
        Assert.assertTrue(verityEmailsSaved(validEmails, tenantId));
        expectedCount += savedEmails.get(emailKey).size();

        Map<String, Set<String>> inValidEmails = initEmailSet(new String[] { "invalid@lattice-engines.com" });
        savedEmails = subscriptionProxy.saveByEmailsAndTenantId(inValidEmails, tenantId);
        Assert.assertEquals(0, savedEmails.get(emailKey).size());
        Assert.assertEquals(expectedCount, getEmailsCount(tenantId));
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCreate" })
    public void testDelete() {
        String tenantId = CustomerSpace.parse(mainTestTenant.getId()).getTenantId();
        Map<String, List<String>> emailMap = subscriptionProxy.getEmailsByTenantId(tenantId);
        List<String> emails = emailMap.get(emailKey);
        Assert.assertTrue(CollectionUtils.isNotEmpty(emails));
        for (String email : emails) {
            subscriptionProxy.deleteByEmailAndTenantId(email, tenantId);
        }
        Assert.assertEquals(0, getEmailsCount(tenantId));
    }

    private int getEmailsCount(String tenantId) {
        Map<String, List<String>> emailMap = subscriptionProxy.getEmailsByTenantId(tenantId);
        List<String> emails = emailMap.get(emailKey);
        return CollectionUtils.isEmpty(emails) ? 0 : emails.size();
    }

    private boolean verityEmailsSaved(Map<String, Set<String>> inputMap, String tenantId) {
        Map<String, List<String>> subscriptMap = subscriptionProxy.getEmailsByTenantId(tenantId);
        Set<String> subscriptEmails = new HashSet<>(subscriptMap.get(emailKey));
        Set<String> inputEmails = inputMap.get(emailKey);
        for (String email : inputEmails) {
            if (!subscriptEmails.contains(email))
                return false;
        }
        return true;
    }

    private Map<String, Set<String>> initEmailSet(String[] array) {
        Map<String, Set<String>> map = new HashMap<>();
        map.put(emailKey, new HashSet<>(Arrays.asList(array)));
        return map;
    }

}
