package com.latticeengines.admin.service.impl;

import java.util.Collection;

import javax.inject.Inject;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.admin.functionalframework.AdminFunctionalTestNGBase;
import com.latticeengines.admin.service.TenantService;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.security.TenantStatus;
import com.latticeengines.domain.exposed.security.TenantType;
public class TenantServiceImplTestNG extends AdminFunctionalTestNGBase {

    @Inject
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

    @Test(groups = "functional")
    public void testUpdateTenantInfo() throws Exception {
        TenantDocument tenantDoc = tenantService.getTenant(TestContractId, TestTenantId);
        TenantInfo tenantInfo = tenantDoc.getTenantInfo();
        Assert.assertNotNull(tenantInfo);
        Assert.assertEquals(tenantInfo.properties.tenantType, null);
        Assert.assertEquals(tenantInfo.properties.status, null);
        tenantInfo.properties.tenantType = TenantType.POC.name();
        tenantInfo.properties.status = TenantStatus.INACTIVE.name();
        tenantService.updateTenantInfo(TestContractId, TestTenantId, tenantInfo);

        TenantDocument anotherTenantDoc = tenantService.getTenant(TestContractId, TestTenantId);
        TenantInfo anotherTenantInfo = anotherTenantDoc.getTenantInfo();
        Assert.assertEquals(anotherTenantInfo.properties.tenantType, TenantType.POC.name());
        Assert.assertEquals(anotherTenantInfo.properties.status, TenantStatus.INACTIVE.name());
        Assert.assertTrue(true);
    }
}
