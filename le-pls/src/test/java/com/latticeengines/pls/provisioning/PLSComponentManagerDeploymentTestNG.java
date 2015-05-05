package com.latticeengines.pls.provisioning;

import java.util.Collections;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.UserService;

public class PLSComponentManagerDeploymentTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private PLSComponentManager componentManager;

    @Autowired
    private TenantService tenantService;

    @Autowired
    private UserService userService;

    @Test(groups= {"deployment"})
    public void installCommonTestTenant() {
        Tenant tenant = createTestTenant();
        List<String> emails = Collections.singletonList("bnguyen@lattice-engines.com");
        componentManager.provisionTenant(tenant, emails);
        Assert.assertTrue(tenantService.hasTenantId(tenant.getId()));

        Tenant newTenant = tenantService.findByTenantId(tenant.getId());
        Assert.assertEquals(newTenant.getName(), tenant.getName());

        for (String email : emails) {
            AccessLevel level = userService.getAccessLevel(tenant.getId(), email);
            Assert.assertEquals(level, AccessLevel.SUPER_ADMIN);
        }
    }

    private Tenant createTestTenant(){
        Tenant tenant = new Tenant();
        tenant.setId("CommonTestContract.TestTenant.Production");
        tenant.setName("Lattice Internal Test Tenant");
        return tenant;
    }

}
