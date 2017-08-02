package com.latticeengines.serviceapps.cdl.testframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.Resource;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.util.SSLUtils;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-serviceapps-cdl-context.xml" })
public abstract class CDLDeploymentTestNGBase extends AbstractTestNGSpringContextTests {

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed testBed;

    protected Tenant mainTestTenant;

    protected void setupTestEnvironmentt() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        SSLUtils.turnOffSSL();
        testBed.bootstrapForProduct(LatticeProduct.CG);
        mainTestTenant = testBed.getMainTestTenant();
        testBed.switchToSuperAdmin();
    }

}
