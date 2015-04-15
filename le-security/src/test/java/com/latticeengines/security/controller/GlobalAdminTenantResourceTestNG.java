package com.latticeengines.security.controller;

import static org.testng.Assert.assertEquals;

import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;

public class GlobalAdminTenantResourceTestNG extends SecurityFunctionalTestNGBase {

    @Test(groups = { "functional", "deployment" })
    public void get() {
        Tenant tenant = restTemplate.getForObject(getRestAPIHostPort() + "/security/globaladmintenant", Tenant.class);
        assertEquals(tenant.getId(), Constants.GLOBAL_ADMIN_TENANT_ID);
        assertEquals(tenant.getName(), Constants.GLOBAL_ADMIN_TENANT_NAME);
    }
}
