package com.latticeengines.baton.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.util.Collection;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

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

    private void assertTenantProperties(Collection<TenantDocument> tenants) {
        TenantDocument tenant = null;
        for (TenantDocument possible : tenants) {
            if (possible.getSpace().getTenantId().equals("TENANT1")) {
                tenant = possible;
                break;
            }
        }
        assertNotNull(tenant);
        assertEquals(tenant.getSpace().getTenantId(), "TENANT1");
        assertEquals(tenant.getTenantInfo().properties.description, "Test tenant");
        assertEquals(tenant.getTenantInfo().properties.displayName, "Tenant for testing");
    }

    @Test(groups = "unit" )
    public void assertDoubleCreateTenant() {
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "Test tenant";
        props.displayName = "Tenant for testing";
        CustomerSpaceInfo info = new CustomerSpaceInfo(props, "");
        assertFalse(batonService.createTenant("CONTRACT1", "TENANT1", CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, info));
    }
}
