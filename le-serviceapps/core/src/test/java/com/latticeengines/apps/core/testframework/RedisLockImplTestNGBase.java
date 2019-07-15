package com.latticeengines.apps.core.testframework;

import javax.annotation.Resource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthFunctionalTestBed;

@Listeners({GlobalAuthCleanupTestListener.class})
@TestExecutionListeners({DirtiesContextTestExecutionListener.class})
@ContextConfiguration(locations = {"classpath:test-serviceapps-core-context.xml"})
public class RedisLockImplTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(RedisLockImplTestNGBase.class);

    @Resource(name = "globalAuthFunctionalTestBed")
    protected GlobalAuthFunctionalTestBed testBed;

    protected Tenant tenant1;
    protected Tenant tenant2;

    protected void setupTestEnvironment() {
        testBed.bootstrap(2);
        tenant1 = testBed.getTestTenants().get(0);
        tenant2 = testBed.getTestTenants().get(1);
        testBed.switchToSuperAdmin();
    }

}
