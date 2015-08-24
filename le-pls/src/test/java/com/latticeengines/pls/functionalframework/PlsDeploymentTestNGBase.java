package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.client.ClientHttpRequestInterceptor;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;

public class PlsDeploymentTestNGBase extends PlsAbstractTestNGBase {
    @Value("${pls.test.deployment.api}")
    private String deployedHostPort;

    @Override
    protected String getRestAPIHostPort() { return getDeployedRestAPIHostPort(); }

    protected String getDeployedRestAPIHostPort() {
        return deployedHostPort.endsWith("/") ? deployedHostPort.substring(0, deployedHostPort.length() - 1) : deployedHostPort;
    }

    protected void setupTestEnvironment() throws NoSuchAlgorithmException, KeyManagementException {
        setTestingTenants();
        loginTestingUsersToMainTenant();
        switchToSuperAdmin();
        turnOffSslChecking();
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
    }

    protected void deleteUserByRestCall(String username) {
        String url = getRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        try {
            sendHttpDeleteForObject(restTemplate, url, ResponseDocument.class);
        } catch (Exception e) {
            // ignore
        }
    }

    protected void createTenantByRestCall(Tenant tenant) {
        try {
            magicRestTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class);
        } catch (Exception e) {
            // ignore
        }
    }

    protected void deleteTenantByRestCall(String tenantId) {
        try {
            sendHttpDeleteForObject(magicRestTemplate, getRestAPIHostPort() + "/pls/admin/tenants/" + tenantId, Boolean.class);
        } catch (Exception e) {
            // ignore
        }
    }


}
