package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.testng.annotations.AfterClass;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    private GlobalAuthDeploymentTestBed deploymentTestBed;

    @Value("${pls.test.deployment.api}")
    private String deployedHostPort;

    @AfterClass(groups = "deployment")
    public void teardown() throws Exception {
        testBed.cleanup();
    }

    @PostConstruct
    private void postConstruct() {
        setTestBed(deploymentTestBed);
    }

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironmentWithOneTenant()
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        testBed.bootstrap(1);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        testBed.bootstrapForProduct(product);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void deleteUserByRestCall(String username) {
        switchToSuperAdmin();
        String url = getRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        restTemplate.delete(url);
    }

}
