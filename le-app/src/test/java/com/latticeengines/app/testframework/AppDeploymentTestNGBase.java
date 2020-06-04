package com.latticeengines.app.testframework;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.aws.dynamo.DynamoService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
    protected String leEnv;

    @Value("${common.le.stack}")
    protected String leStack;

    @Value("${common.test.pls.url}")
    private String deployedPLSHostPort;

    protected Tenant mainTestTenant;
    protected CustomerSpace mainTestCustomerSpace;

    protected String getPLSRestAPIHostPort() {
        return deployedPLSHostPort.endsWith("/") ? deployedPLSHostPort.substring(0, deployedPLSHostPort.length() - 1)
                : deployedPLSHostPort;
    }

    protected void setupTestEnvironmentWithOneTenant(Map<String, Boolean> featureFlagMap) {
        testBed.bootstrapForProduct(LatticeProduct.CG, featureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
    }

    protected void setupTestEnvironmentWithOneTenant() {
        testBed.bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        mainTestCustomerSpace = CustomerSpace.parse(mainTestTenant.getId());
        MultiTenantContext.setTenant(mainTestTenant);
    }
}
