package com.latticeengines.pls.controller;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;

public class QuotaResourceTestNG extends PlsFunctionalTestNGBase {

    private static final String PLS_QUOTA_URL = "pls/quotas/";

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        QUOTA.setId(TEST_QUOTA_ID);
        QUOTA.setBalance(BALANCE);

        setupUsers();
        cleanupQuotaDB();
    }

    @BeforeMethod(groups = { "functional" })
    public void beforeMethod() {
        // using admin session by default
        switchToSuperAdmin();
    }

    @Test(groups = { "functional" })
    public void postQuota_calledWithQuota_assertQuotaIsPosted() {
        restTemplate.postForObject(getRestAPIHostPort() + PLS_QUOTA_URL, QUOTA,
                Quota.class);

        Quota quota = restTemplate.getForObject(getRestAPIHostPort()
                + PLS_QUOTA_URL + TEST_QUOTA_ID, Quota.class);
        assertNotNull(quota);
        assertEquals(quota.getId(), TEST_QUOTA_ID);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "postQuota_calledWithQuota_assertQuotaIsPosted" })
    public void updateQuota_calledWithParameters_assertQuotaIsUpdated() {
        QUOTA.setBalance(BALANCE_1);

        restTemplate.put(getRestAPIHostPort() + PLS_QUOTA_URL + TEST_QUOTA_ID,
                QUOTA);

        Quota quota = restTemplate.getForObject(getRestAPIHostPort()
                + PLS_QUOTA_URL + TEST_QUOTA_ID, Quota.class);
        assertNotNull(quota);
        assertEquals(quota.getId(), TEST_QUOTA_ID);
        assertEquals(quota.getBalance(), BALANCE_1);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "updateQuota_calledWithParameters_assertQuotaIsUpdated" })
    public void deleteQuota_calledInCurrentTenant_assertQuotaIsDeleted() {
        restTemplate.delete(getRestAPIHostPort() + PLS_QUOTA_URL
                + TEST_QUOTA_ID);

        Quota quota = restTemplate.getForObject(getRestAPIHostPort()
                + PLS_QUOTA_URL + TEST_QUOTA_ID, Quota.class);
        assertNull(quota);
    }
}
