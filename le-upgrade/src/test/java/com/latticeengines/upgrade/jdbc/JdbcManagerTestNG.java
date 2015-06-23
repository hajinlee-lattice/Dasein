package com.latticeengines.upgrade.jdbc;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class JdbcManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private TenantModelJdbcManager tenantModelJdbcManager;

    @Test(groups = "functional")
    public void testGetAllTenantsToBeUpgraded() {
        List<String> tenants = tenantModelJdbcManager.getTenantsToUpgrade();
        Assert.assertTrue(tenants.size() > 0, "Found no tenants to be upgraded.");
    }

    @Test(groups = "functional")
    public void testGetModelToBeUpgraded() {
        String modelGuid = tenantModelJdbcManager.getModelToUpgrade(CUSTOMER);
        Assert.assertEquals(modelGuid, MODEL_GUID);
    }

}


