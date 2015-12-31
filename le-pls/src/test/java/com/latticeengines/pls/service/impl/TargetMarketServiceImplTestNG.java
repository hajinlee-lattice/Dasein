package com.latticeengines.pls.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TargetMarketEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TargetMarketService;
import com.latticeengines.security.exposed.service.TenantService;

public class TargetMarketServiceImplTestNG extends PlsFunctionalTestNGBase {
    
    private static final String TENANT1 = "TENANT1";
    private static final String TENANT2 = "TENANT2";
    
    @Autowired
    private TargetMarketService targetMarketService;
    
    @Autowired
    private TargetMarketEntityMgr targetMarketEntityMgr;

    @Autowired
    private TenantService tenantService;
    
    @DataProvider(name = "tenants")
    public Object[][] getTenants() {
        return new Object[][] { //
                new Object[] { TENANT1, "SomeTargetMarket" }, //
                new Object[] { TENANT2, "SomeTargetMarket" } //
        };
    }
    
    private void setupTenant(String t) throws Exception {
        Tenant tenant = tenantService.findByTenantId(t);
        if (tenant != null) {
            tenantService.discardTenant(tenant);
        }
        tenant = new Tenant();
        tenant.setId(t);
        tenant.setName(t);
        tenantService.registerTenant(tenant);

        setupSecurityContext(tenant);
        cleanupTargetMarkets();
    }

    private void cleanupTargetMarkets() {
        List<TargetMarket> targetMarkets = targetMarketEntityMgr.findAllTargetMarkets();
        
        for (TargetMarket targetMarket : targetMarkets) {
            targetMarketEntityMgr.delete(targetMarket);
        }
    }
    
    @Test(groups = "functional", dataProvider = "tenants")
    public void createDefaultTargetMarketWithTenant(String t, String targetMarketName) throws Exception {
        setupTenant(t);
        
        TargetMarket targetMarket = targetMarketService.createDefaultTargetMarket();
        assertNotNull(targetMarket);
        assertTrue(targetMarket.getIsDefault());
        assertNotNull(targetMarket.getCreationTimestamp());
    }

    @Test(groups = "functional", dataProvider = "tenants")
    public void createTargetMarketWithTenantAndTargetMarket(String t, String targetMarketName) throws Exception {
        setupTenant(t);
        
        TargetMarket targetMarket = new TargetMarket();
        targetMarket.setName(targetMarketName);
        targetMarket.setDescription("Target market with name " + targetMarketName);
        targetMarket.setOffset(0);
        targetMarket.setIsDefault(false);
        targetMarket.setEventColumnName("");
        targetMarketService.createTargetMarket(targetMarket);
        TargetMarket retrievedTargetMarket = targetMarketService.findTargetMarketByName(targetMarketName);
        assertEquals(retrievedTargetMarket.getName(), targetMarketName);
        assertNotNull(retrievedTargetMarket.getCreationTimestamp());
    }

}
