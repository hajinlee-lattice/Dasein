package com.latticeengines.upgrade.jdbc;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class TenantModelJdbcManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private TenantModelJdbcManager tenantModelJdbcManager;

    @Test(groups = "functional")
    public void testGetAllTenantsToBeUpgraded() {
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        Assert.assertTrue(tenants.size() > 0, "Found no tenants to be upgraded.");
    }

    @Test(groups = "functional")
    public void testGetActiveModels() {
        List<String> modelGuids = tenantModelJdbcManager.getActiveModels(CUSTOMER);
        Assert.assertEquals(modelGuids.get(0), MODEL_GUID);
    }

    @Test(groups = "functional")
    public void testGetUuids() {
        List<String> uuids = tenantModelJdbcManager.getActiveUuids(CUSTOMER);
        Assert.assertEquals(uuids.get(0), UUID);
    }

    @Test(groups = "functional")
    public void testModelIsActive() {
        Assert.assertTrue(tenantModelJdbcManager.modelIsActive(CUSTOMER, UUID));
        Assert.assertTrue(tenantModelJdbcManager.modelIsActive(CUSTOMER, UUID));
        Assert.assertFalse(tenantModelJdbcManager.modelIsActive(CUSTOMER, java.util.UUID.randomUUID().toString()));
    }
}


