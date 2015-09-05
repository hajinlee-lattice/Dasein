package com.latticeengines.pls.provisioning;

import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.service.UserService;

public class PLSComponentManagerTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private PLSComponentManager componentManager;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Test(groups= {"functional"})
    public void testProvisionTenant() {
        Tenant tenant = createTestTenant();
        List<String> superAdmins = Collections.singletonList("bnguyen@lattice-engines.com");
        List<String> latticeAdmins = Collections.singletonList("ysong@lattice-engines.com");
        componentManager.provisionTenant(tenant, superAdmins, latticeAdmins);
        Assert.assertTrue(tenantService.hasTenantId(tenant.getId()));

        Tenant newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), tenant.getName());

        for (String email : superAdmins) {
            AccessLevel level = userService.getAccessLevel(tenant.getId(), email);
            Assert.assertEquals(level, AccessLevel.SUPER_ADMIN);
        }
        for (String email : latticeAdmins) {
            AccessLevel level = userService.getAccessLevel(tenant.getId(), email);
            Assert.assertEquals(level, AccessLevel.INTERNAL_ADMIN);
        }

        tenant = createTestTenant();
        tenant.setName("new name");
        componentManager.provisionTenant(tenant, Collections.<String>emptyList(), Collections.<String>emptyList());
        Assert.assertTrue(tenantService.hasTenantId(tenant.getId()));

        newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), "new name");

        componentManager.discardTenant(tenant);
        Assert.assertFalse(tenantService.hasTenantId(tenant.getId()));

        for (String email : superAdmins) {
            Assert.assertFalse(userService.inTenant(tenant.getId(), email));
        }
        for (String email : latticeAdmins) {
            Assert.assertFalse(userService.inTenant(tenant.getId(), email));
        }
    }

    private Tenant createTestTenant(){
        Tenant tenant = new Tenant();
        tenant.setId("PLS_COMPONENT_MANAGER_TEST_TENANT");
        tenant.setName("Pls component manager test tenant");
        return tenant;
    }

}
