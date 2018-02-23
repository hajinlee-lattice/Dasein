package com.latticeengines.pls.entitymanager.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.pls.entitymanager.QuotaEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
public class QuotaEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    QuotaEntityMgr quotaEntityMgr;

    @BeforeClass(groups = { "functional" })
    public void setup() throws Exception {
        QUOTA.setId(TEST_QUOTA_ID);
        QUOTA.setBalance(BALANCE);
        setupMarketoEloquaTestEnvironment();
        cleanupQuotaDB();
    }

    @Test(groups = { "functional" })
    public void create_calledWithParameters_assertQuotaIsCreated() throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);
        assertNull(this.quotaEntityMgr.findQuotaByQuotaId(TEST_QUOTA_ID));

        this.quotaEntityMgr.create(QUOTA);

        Thread.sleep(500); // wait for replication lag
        Quota quota = this.quotaEntityMgr.findQuotaByQuotaId(TEST_QUOTA_ID);
        assertNotNull(quota);
        assertEquals(quota.getId(), TEST_QUOTA_ID);
    }

    @Test(groups = { "functional" }, dependsOnMethods = { "create_calledWithParameters_assertQuotaIsCreated" })
    public void createdQuotaInOneTenant_findQuotaInAnotherTenant_quotaCannotBeFound() {
//        MultiTenantContext.setTenant(testTenants().get(1));
//
//        Quota quota = this.quotaEntityMgr.findQuotaByQuotaId(TEST_QUOTA_ID);
//
//        assertNull(quota);
    }

    @Test(groups = {"functional" }, dependsOnMethods = { "createdQuotaInOneTenant_findQuotaInAnotherTenant_quotaCannotBeFound" })
    public void update_calledWithParameters_assertQuotaIsUpdated() throws Exception {
        MultiTenantContext.setTenant(mainTestTenant);

        QUOTA.setPid(null);
        QUOTA.setBalance(BALANCE_1);
        this.quotaEntityMgr.updateQuotaByQuotaId(QUOTA, TEST_QUOTA_ID);
        Thread.sleep(500); // wait for replication lag
        Quota quota = this.quotaEntityMgr.findQuotaByQuotaId(TEST_QUOTA_ID);
        assertNotNull(quota);
        assertEquals(quota.getBalance(), BALANCE_1);
    }
    
    @Test(groups = { "functional" }, dependsOnMethods = { "update_calledWithParameters_assertQuotaIsUpdated" })
    public void delete_calledWithParameters_assertQuotaIsDeleted() throws Exception {
        Quota quota = this.quotaEntityMgr.findQuotaByQuotaId(TEST_QUOTA_ID);
        assertNotNull(quota);

        this.quotaEntityMgr.deleteQuotaByQuotaId(TEST_QUOTA_ID);
        Thread.sleep(500); // wait for replication lag
        assertNull(this.quotaEntityMgr.findQuotaByQuotaId(TEST_QUOTA_ID));
    }

}
