package com.latticeengines.saml.testframework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.security.exposed.util.MultiTenantContext;
import com.latticeengines.testframework.security.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.security.impl.GlobalAuthFunctionalTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-saml-context.xml" })
public abstract class SamlTestNGBase extends AbstractTestNGSpringContextTests {
    public static final String RESOURCE_BASE = "/com/latticeengines/saml/";

    @Autowired
    protected GlobalAuthFunctionalTestBed globalAuthFunctionalTestBed;

    protected void setupTenant() {
        // Create test tenant
        globalAuthFunctionalTestBed.bootstrap(2);
        MultiTenantContext.setTenant(globalAuthFunctionalTestBed.getMainTestTenant());

        // Create test user
        globalAuthFunctionalTestBed.switchToInternalAdmin();
    }

}
