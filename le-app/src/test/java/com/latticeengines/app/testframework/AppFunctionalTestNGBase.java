package com.latticeengines.app.testframework;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-app-context.xml" })
public class AppFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    @Inject
    protected GlobalAuthFunctionalTestBed globalAuthFunctionalTestBed;

    protected Tenant mainTestTenant;

    protected void setupTestEnvironmentWithOneTenant() {
        globalAuthFunctionalTestBed.bootstrap(1);
        mainTestTenant = globalAuthFunctionalTestBed.getMainTestTenant();
        MultiTenantContext.setTenant(mainTestTenant);
    }
}
