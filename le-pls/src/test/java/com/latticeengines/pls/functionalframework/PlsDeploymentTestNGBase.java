package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;

import org.testng.Assert;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {
    @Value("${pls.test.deployment.api}")
    private String deployedHostPort;

    @Override
    protected String getRestAPIHostPort() {
        return getDeployedRestAPIHostPort();
    }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1)
                : deployedHostPort;
    }

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException, IOException {
        turnOffSslChecking();
        resetTenantsViaTenantConsole();

        setTestingTenants();
        loginTestingUsersToMainTenant();
        switchToSuperAdmin();
    }

    protected void resetTenantsViaTenantConsole() throws IOException {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String response = sendHttpPutForObject(magicRestTemplate, getRestAPIHostPort() + "/pls/internal/testtenants/",
                "", String.class);
        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);
        Assert.assertTrue(json.get("Success").asBoolean());
    }

    protected void deleteUserByRestCall(String username) {
        String url = getRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        sendHttpDeleteForObject(restTemplate, url, ResponseDocument.class);
    }

    protected void createTenantByRestCall(Tenant tenant) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        magicRestTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class);
    }

    protected void deleteTenantByRestCall(String tenantId) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        sendHttpDeleteForObject(magicRestTemplate, getRestAPIHostPort() + "/pls/admin/tenants/" + tenantId,
                Boolean.class);
    }

}
