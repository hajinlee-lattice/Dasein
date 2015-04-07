package com.latticeengines.baton.exposed.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.AbstractMap;
import java.util.List;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.CamilleConfiguration;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.CamilleEnvironment.Mode;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;

public class BatonServiceImplUnitTestNG {
    
    private BatonService batonService = new BatonServiceImpl();

    
    @BeforeClass(groups = "unit")
    public void setup() throws Exception {
        CamilleConfiguration config = new CamilleConfiguration();
        config.setConnectionString("localhost:2181");
        config.setPodId("ignored");

        CamilleEnvironment.start(Mode.BOOTSTRAP, config);

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
        batonService.deleteTenant("CONTRACT1", "TENANT1");
        List<AbstractMap.SimpleEntry<String, TenantInfo>> tenants = batonService.getTenants(null);
        assertEquals(tenants.size(), 0);
    }

    @Test(groups = "unit")
    public void getTenantsWithContractId() {
        assertSingleTenantProperties(batonService.getTenants("CONTRACT1"));
    }

    @Test(groups = "unit")
    public void getTenantsWithNoContractId() {
        assertSingleTenantProperties(batonService.getTenants(null));
    }
    
    @Test(groups = "unit")
    public void deleteNonExistingTenant() {
        assertFalse(batonService.deleteTenant("CONTRACT1", "xyz"));
    }
    
    private void assertSingleTenantProperties(List<AbstractMap.SimpleEntry<String, TenantInfo>> tenants) {
        assertEquals(tenants.size(), 1);
        assertEquals(tenants.get(0).getKey(), "TENANT1");
        assertEquals(tenants.get(0).getValue().properties.description, "Test tenant");
        assertEquals(tenants.get(0).getValue().properties.displayName, "Tenant for testing");
    }
}
