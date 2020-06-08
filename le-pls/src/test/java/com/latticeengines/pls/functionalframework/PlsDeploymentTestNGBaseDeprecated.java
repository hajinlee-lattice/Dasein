package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;

import org.springframework.beans.factory.annotation.Value;

import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Segment;

public class PlsDeploymentTestNGBaseDeprecated extends PlsAbstractTestNGBaseDeprecated {

    @Value("${common.test.pls.url}")
    private String deployedHostPort;

    @Value("${pls.test.deployment.reset.by.admin:true}")
    private boolean resetByAdminApi;

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    private String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment(null, false);
    }

    protected void setupTestEnvironment(String productPrefix, Boolean forceInstallation)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        setTestingTenants();
        loginTestingUsersToMainTenant();
        switchToSuperAdmin();
    }

    protected void setupSecurityContext(ModelSummary summary) {
        setupSecurityContext(summary.getTenant());
    }

    protected void setupSecurityContext(Segment segment) {
        setupSecurityContext(segment.getTenant());
    }

}
