package com.latticeengines.dante.testFramework;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-dante-context.xml" })
public class DanteTestNGBase extends AbstractTestNGSpringContextTests {
    @Autowired
    protected GlobalAuthFunctionalTestBed testBed;

    protected Tenant mainTestTenant;

    @BeforeClass(groups = { "functional", "deployment" })
    public void setupRunEnvironment() throws Exception {
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
    }
}
