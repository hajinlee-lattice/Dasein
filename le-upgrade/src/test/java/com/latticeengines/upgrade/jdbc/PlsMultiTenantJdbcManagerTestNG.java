package com.latticeengines.upgrade.jdbc;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class PlsMultiTenantJdbcManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private PlsMultiTenantJdbcManager plsMultiTenantJdbcManager;

    @BeforeClass(groups = "functional")
    private void setup() {
        tearDown();
        plsGaManager.registerTenant(CUSTOMER);
        plsGaManager.setupAdminUsers(CUSTOMER);
        uploadModel();
        uploadModel();
        uploadModel();
        plsMultiTenantJdbcManager.getModelGuids();
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        String url = plsApiHost + "/pls/admin/tenants/" + TUPLE_ID;
        magicRestTemplate.delete(url);
        Assert.assertFalse(tenantExists(), "Test tenant should be deleted.");
    }

    @Test(groups = "functional")
    public void testHasUuid() {
        Assert.assertTrue(plsMultiTenantJdbcManager.hasUuid(UUID), "Should find the UUID of uploaded model");
    }

    @Test(groups = "functional")
    public void testFindName() {
        String name = plsMultiTenantJdbcManager.findNameByUuid(UUID);
        Assert.assertTrue(StringUtils.isEmpty(name), "The name should not be customized.");

        plsGaManager.updateModelName(MODEL_GUID, "New Name");
        name = plsMultiTenantJdbcManager.findNameByUuid(UUID);
        Assert.assertEquals(name, "New Name");
    }

    @Test(groups = "functional")
    public void testFindModelGuid() {
        String modelGuid = plsMultiTenantJdbcManager.findModelGuidByUuid(UUID);
        Assert.assertEquals(modelGuid, MODEL_GUID);
    }

    @Test(groups = "functional", dependsOnMethods = {"testHasUuid", "testFindName", "testFindModelGuid"})
    public void testDeleteModelSummariesByUuid() {
        setup();

        Assert.assertTrue(plsMultiTenantJdbcManager.hasUuid(UUID), "Should find the UUID of uploaded model");
        plsMultiTenantJdbcManager.deleteModelSummariesByUuid(UUID);
        plsMultiTenantJdbcManager.getModelGuids();
        Assert.assertFalse(plsMultiTenantJdbcManager.hasUuid(UUID), "Should not have the UUID of uploaded model");
    }

    @Test(groups = "functional", dependsOnMethods = {"testHasUuid", "testFindName", "testFindModelGuid"})
    public void testDeleteModelSummariesBYTenant() {
        setup();

        Assert.assertTrue(plsMultiTenantJdbcManager.hasUuid(UUID), "Should find the UUID of uploaded model");
        plsMultiTenantJdbcManager.deleteModelSummariesByTenantId(TUPLE_ID);
        plsMultiTenantJdbcManager.getModelGuids();
        Assert.assertFalse(plsMultiTenantJdbcManager.hasUuid(UUID), "Should not have the UUID of uploaded model");
    }
}


