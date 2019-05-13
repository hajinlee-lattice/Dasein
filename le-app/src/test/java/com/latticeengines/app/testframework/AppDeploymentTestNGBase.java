package com.latticeengines.app.testframework;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-app-context.xml" })
public class AppDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    @Inject
    protected GlobalAuthDeploymentTestBed testBed;

    @Inject
    protected DynamoService dynamoService;

    @Value("${common.le.environment}")
    private String leEnv;

    @Value("${common.le.stack}")
    private String leStack;

    protected Tenant mainTestTenant;

    protected void setupTestEnvironmentWithOneTenant() {
        testBed.bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
    }
}
