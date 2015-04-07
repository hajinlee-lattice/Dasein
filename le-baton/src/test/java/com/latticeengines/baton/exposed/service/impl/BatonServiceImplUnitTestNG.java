package com.latticeengines.baton.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.util.AbstractMap;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public class BatonServiceImplUnitTestNG {

    private BatonService batonService = new BatonServiceImpl();

    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        CamilleTestEnvironment.start();

        if (ContractLifecycleManager.exists("CONTRACT1")) {
            ContractLifecycleManager.delete("CONTRACT1");
        }
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "Test tenant";
        props.displayName = "Tenant for testing";
        CustomerSpaceInfo info = new CustomerSpaceInfo(props, "");

        batonService.createTenant("CONTRACT1", "TENANT1", CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, info);
    }

    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit")
    public void getTenantsWithContractId() {
        assertTenantProperties(batonService.getTenants("CONTRACT1"));
    }

    @Test(groups = "unit")
    public void getTenantsWithNoContractId() {
        assertTenantProperties(batonService.getTenants(null));
    }

    @Test(groups = "unit")
    public void deleteNonExistingTenant() {
        assertFalse(batonService.deleteTenant("CONTRACT1", "xyz"));
    }

    private void assertTenantProperties(List<AbstractMap.SimpleEntry<String, TenantInfo>> tenants) {
        AbstractMap.SimpleEntry<String, TenantInfo> tenant = null;
        for (AbstractMap.SimpleEntry<String, TenantInfo> possible : tenants) {
            if (possible.getKey().equals("TENANT1")) {
                tenant = possible;
                break;
            }
        }
        assertNotNull(tenant);
        assertEquals(tenant.getKey(), "TENANT1");
        assertEquals(tenant.getValue().properties.description, "Test tenant");
        assertEquals(tenant.getValue().properties.displayName, "Tenant for testing");
    }
}
