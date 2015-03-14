package com.latticeengines.pls.setup;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.globalauth.authentication.GlobalTenantManagementService;
import com.latticeengines.pls.security.GrantedRight;

public class SetupTestEnvironmentTestNG extends PlsFunctionalTestNGBase {
    private static final Log log = LogFactory.getLog(SetupTestEnvironmentTestNG.class);

    @Autowired
    private GlobalTenantManagementService globalTenantManagementService;

    @BeforeClass(groups = "functional.production", enabled = true)
    public void setup() throws Exception {
        createUser(generalUsername, generalUsername, "General", "User", generalPasswordHash);
        createUser(adminUsername, adminUsername, "Super", "User", adminPasswordHash);
        List<Tenant> tenants = getTenants(5);

        for (Tenant tenant : tenants) {
            try {
                globalTenantManagementService.registerTenant(tenant);
            } catch (Exception e) {
                log.info("Ignoring tenant registration error");
            }

            for (GrantedRight right : GrantedRight.getAdminRights()) {
                grantRight(right, tenant.getId(), adminUsername);
            }

            for (GrantedRight right : GrantedRight.getDefaultRights()) {
                grantRight(right, tenant.getId(), generalUsername);
            }
        }

    }

    private List<Tenant> getTenants(int numTenants) {
        List<Tenant> tenants = new ArrayList<>();

        for (int i = 0; i < numTenants; i++) {
            Tenant tenant = new Tenant();
            String t = "TENANT" + (i + 1);
            tenant.setId(t);
            tenant.setName(t);
            tenants.add(tenant);
        }
        return tenants;
    }

    @Test(groups = "functional.production")
    public void test() {

    }
}
