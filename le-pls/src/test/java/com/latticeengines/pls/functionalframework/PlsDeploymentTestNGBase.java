package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.testng.Assert;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.security.exposed.Constants;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {
    @Value("${pls.test.deployment.api}")
    private String deployedHostPort;

    @Value("${pls.test.deployment.reset.by.admin:true}")
    private boolean resetByAdminApi;

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }
    
    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        setupTestEnvironment(null);
    }

    protected void setupTestEnvironment(String productPrefix) throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        resetTenantsViaTenantConsole(productPrefix);

        setTestingTenants();
        loginTestingUsersToMainTenant();
        switchToSuperAdmin();
    }

    protected void resetTenantsViaTenantConsole(String productPrefix) throws IOException {
        if (resetByAdminApi) {
            addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
            magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
            String url = "/pls/internal/testtenants";
            if (productPrefix != null) {
                url += "?product=" + productPrefix;
            } else {
                url += "/";
            }
            String response = sendHttpPutForObject(magicRestTemplate, getRestAPIHostPort()
                    + url, "", String.class);
            ObjectMapper mapper = new ObjectMapper();
            JsonNode json = mapper.readTree(response);
            Assert.assertTrue(json.get("Success").asBoolean());
        }
    }
    
}
