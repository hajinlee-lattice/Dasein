package com.latticeengines.admin.service.impl;

import java.util.Collection;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;

public class TenantServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Test(groups = "functional")
    public void testSetupSpaceConfiguration() throws Exception {
        TenantDocument tenantDoc = tenantService.getTenant(TestContractId, TestTenantId);
        SpaceConfiguration spaceConfig = tenantDoc.getSpaceConfig();
        Assert.assertNotNull(spaceConfig);

        Assert.assertEquals(spaceConfig.getTopology(), CRMTopology.MARKETO);

        Assert.assertTrue(true);
    }

    @Test(groups = "functional")
    public void testGetTenants() throws Exception {
        Collection<TenantDocument> collection = tenantService.getTenantsInCache(null);
        Assert.assertNotNull(collection);
        Assert.assertEquals(collection.size() > 0, true);
    }
}
