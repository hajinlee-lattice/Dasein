package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.security.exposed.Constants;

public class PlsDeploymentTestNGBaseDeprecated extends PlsAbstractTestNGBaseDeprecated {

    protected static final Log log = LogFactory.getLog(PlsDeploymentTestNGBaseDeprecated.class);

    @Value("${pls.test.deployment.api}")
    private String deployedHostPort;

    @Value("${pls.test.deployment.reset.by.admin:true}")
    private boolean resetByAdminApi;

    @Value("${pls.internal.admin.api}")
    private String adminApi;

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment(null, false);
    }

    protected void setupTestEnvironment(String productPrefix, Boolean forceInstallation)
            throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        resetTenantsViaTenantConsole(productPrefix, forceInstallation);

        setTestingTenants();
        loginTestingUsersToMainTenant();
        switchToSuperAdmin();
    }

    protected void resetTenantsViaTenantConsole(String productPrefix, Boolean forceInstallation) throws IOException {
        if (resetByAdminApi) {
            addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
            magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
            String url = "/pls/internal/testtenants/?forceinstall=" + String.valueOf(forceInstallation);
            if (productPrefix != null) {
                url += "&product=" + productPrefix;
            }
            String response = sendHttpPutForObject(magicRestTemplate, getRestAPIHostPort() + url, "", String.class);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(response);
            Assert.assertTrue(json.get("Success").asBoolean());
        }
    }

}
