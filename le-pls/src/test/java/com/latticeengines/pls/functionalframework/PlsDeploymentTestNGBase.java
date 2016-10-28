package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {

    @Autowired
    @Qualifier(value = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

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

    protected void setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product,
            Map<String, Boolean> featureFlagMap) throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        testBed.bootstrapForProduct(product, featureFlagMap);
        mainTestTenant = testBed.getMainTestTenant();
        switchToSuperAdmin();
    }

    protected void deleteUserByRestCall(String username) {
        switchToSuperAdmin();
        String url = getRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        restTemplate.delete(url);
    }

}
