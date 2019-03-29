package com.latticeengines.auth.testframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;

import com.latticeengines.auth.exposed.entitymanager.GlobalAuthTenantEntityMgr;
import com.latticeengines.auth.exposed.entitymanager.GlobalAuthUserEntityMgr;
import com.latticeengines.domain.exposed.auth.GlobalAuthTenant;
import com.latticeengines.domain.exposed.auth.GlobalAuthUser;
import com.latticeengines.domain.exposed.camille.CustomerSpace;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-auth-context.xml" })
public class AuthFunctionalTestNGBase extends AbstractTestNGSpringContextTests {


    @Autowired
    protected GlobalAuthTenantEntityMgr gaTenantEntityMgr;

    @Autowired
    protected GlobalAuthUserEntityMgr gaUserEntityMgr;

    protected GlobalAuthUser createGlobalAuthUser() {
        GlobalAuthUser gAuthUser = new GlobalAuthUser();
        gAuthUser.setFirstName("Lattice" + System.currentTimeMillis());
        gAuthUser.setLastName("TestUser");
        gaUserEntityMgr.create(gAuthUser);
        return gAuthUser;
    }

    protected GlobalAuthTenant createGlobalAuthTenant() {
        GlobalAuthTenant gAuthTenant = new GlobalAuthTenant();
        String fullTenantId = "LeTest" + System.currentTimeMillis();
        String tenantId = CustomerSpace.parse(fullTenantId).toString();
        gAuthTenant.setName(fullTenantId);
        gAuthTenant.setId(tenantId);
        gaTenantEntityMgr.create(gAuthTenant);
        return gAuthTenant;
    }
}
