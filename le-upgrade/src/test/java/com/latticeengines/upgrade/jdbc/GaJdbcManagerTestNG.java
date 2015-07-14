package com.latticeengines.upgrade.jdbc;

import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.upgrade.functionalframework.UpgradeFunctionalTestNGBase;

public class GaJdbcManagerTestNG extends UpgradeFunctionalTestNGBase {

    @Autowired
    private GaJdbcManager gaJdbcManager;

    @Autowired
    @Qualifier("gaJdbcTemlate")
    private JdbcTemplate gaJdbcTemlate;

    private String pid;

    @BeforeClass(groups = "functional")
    private void setup() {
        deleteTenant(CUSTOMER);
        insertTenant(CUSTOMER, CUSTOMER);
    }

    @AfterClass(groups = "functional")
    public void tearDown() {
        deleteTenantByPid(pid);
    }

    @Test(groups = "functional")
    public void testGetPid() {
        pid = gaJdbcManager.getTenantPid(CUSTOMER);
        Assert.assertNotNull(pid, "Should have a PID of inserted tenant");

        Tenant tenant = getTenantByPID(pid);
        Assert.assertEquals(tenant.getId(), CUSTOMER);
        Assert.assertEquals(tenant.getName(), CUSTOMER);
    }

    @Test(groups = "functional")
    public void testGetPidForNonExistingTenant() {
        String pid = gaJdbcManager.getTenantPid("nope");
        Assert.assertNull(pid, "Should not have a PID of non-existing tenant");
    }

    @Test(groups = "functional", dependsOnMethods = "testGetPid")
    public void testSubstituteId() {
        gaJdbcManager.substituteSingularIdByTupleId(CUSTOMER);
        String pid2 = gaJdbcManager.getTenantPid(TUPLE_ID);
        Assert.assertEquals(pid, pid2);

        Tenant tenant = getTenantByPID(pid);
        Assert.assertEquals(tenant.getId(), TUPLE_ID);
        Assert.assertEquals(tenant.getName(), CUSTOMER);
    }

    private void deleteTenant(String deploymentId) {
        gaJdbcTemlate.execute("DELETE FROM GlobalTenant WHERE Deployment_ID = \'" + deploymentId + "\'");
    }

    private void deleteTenantByPid(String pid) {
        gaJdbcTemlate.execute("DELETE FROM GlobalTenant WHERE GlobalTenant_ID = \'" + pid + "\'");
    }

    private void insertTenant(String deploymentId, String displayName) {
        gaJdbcTemlate.execute("INSERT GlobalTenant (" +
                "Deployment_ID, Display_Name, Creation_Date, Last_Modification_Date, Created_By, Last_Modified_By" +
                ") VALUES (" +
                "\'" + deploymentId + "\', \'" + displayName + "\', " +
                "GETUTCDATE(), GETUTCDATE(), 0, 0)");
    }

    private Tenant getTenantByPID(String pid) {
        Tenant tenant = new Tenant();
        Map<String, Object> result = gaJdbcTemlate.queryForMap("SELECT Deployment_ID, Display_Name FROM GlobalTenant"
                + " WHERE GlobalTenant_ID = \'" + pid + "\'");
        tenant.setId(result.get("Deployment_ID").toString());
        tenant.setName(result.get("Display_Name").toString());
        return tenant;
    }
}


