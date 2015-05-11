package com.latticeengines.admin.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;

public class TenantServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Autowired
    private TenantService tenantService;

    @Test(groups = "functional")
    public void testSetupSpaceConfiguration() throws Exception {
        TenantDocument tenantDoc = tenantService.getTenant(TestContractId, TestTenantId);
        SpaceConfiguration spaceConfig = tenantDoc.getSpaceConfig();
        Assert.assertNotNull(spaceConfig);

        Assert.assertEquals(spaceConfig.getTopology(), CRMTopology.MARKETO);
        Assert.assertEquals(spaceConfig.getProduct(), LatticeProduct.LPA);

        Assert.assertTrue(true);
    }

    @Test(groups = "functional")
    public void testSetupSpaceConfigSchema() throws Exception {
        DocumentDirectory dir = tenantService.getSpaceConfigSchema();
        Assert.assertEquals(dir.getChild("Topology").getDocument().getData(),
                "[\"Marketo\",\"Eloqua\",\"SFDC\"]");
    }
}
