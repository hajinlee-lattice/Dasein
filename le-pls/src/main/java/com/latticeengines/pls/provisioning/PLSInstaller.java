package com.latticeengines.pls.provisioning;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.web.client.RestTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.config.bootstrap.LatticeComponentInstaller;
import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;

public class PLSInstaller extends LatticeComponentInstaller {

    private static final Log LOGGER = LogFactory.getLog(PLSInstaller.class);

    private static RestTemplate restTemplate;

    private static String RESTAPI_HOST_PORT = PropertyUtils.getProperty("pls.api.hostport");

    public PLSInstaller() {
        super(PLSComponent.componentName);
        configRestTemplate();
    }

    private static void configRestTemplate() {
        if (restTemplate == null) {
            restTemplate = new RestTemplate();
            MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader =
                    new MagicAuthenticationHeaderHttpRequestInterceptor(Constants.INTERNAL_SERVICE_HEADERVALUE);
            restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
        }
    }

    @Override
    public void installCore(CustomerSpace space, String serviceName, int dataVersion, DocumentDirectory configDir) {
        if (!serviceName.equals(PLSComponent.componentName)) { return; }

        // get tenant information
        String tenantId = space.getTenantId();
        String tenantName, emailListInJson;
        List<String> adminEmails = new ArrayList<>();
        try {
            emailListInJson = configDir.get("/AdminEmails").getDocument().getData();
            tenantName = configDir.get("/TenantName").getDocument().getData();
        } catch (NullPointerException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse input configuration", e);
        }

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode aNode = mapper.readTree(emailListInJson);
            if (!aNode.isArray()) {
                throw new IOException("AdminEmails suppose to be a list of strings");
            }
            for (JsonNode node : aNode) {
                adminEmails.add(node.asText());
            }
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_18028, "Cannot parse AdminEmails to a list of valid emails", e);
        }


        Tenant tenant = new Tenant();
        tenant.setId(tenantId);
        tenant.setName(tenantName);

        LOGGER.info(String.format("Provisioning tenant %s through API %s", tenantId, RESTAPI_HOST_PORT));

        Throwable e = new LedpException(LedpCode.LEDP_18028, new String[]{tenantId});

        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        HttpEntity<JsonNode> requestEntity = new HttpEntity<>(null, headers);
        try {
            ResponseEntity<Boolean> responseEntity = restTemplate.exchange(
                    RESTAPI_HOST_PORT + "/pls/admin/tenants/" + tenantId,
                    HttpMethod.DELETE,
                    requestEntity,
                    Boolean.class
            );
            if (!responseEntity.getBody()) {
                throw new LedpException(LedpCode.LEDP_18028, "Failed to delete the existing tenant, if any", e);
            }
        } catch (Exception ex) {
            //ignore
        }

        boolean tenantCreated =
                restTemplate.postForObject(RESTAPI_HOST_PORT + "/pls/admin/tenants", tenant, Boolean.class);

        if (!tenantCreated) {
            throw new LedpException(LedpCode.LEDP_18028, "Failed to create the requested tenant " + tenantId, e);
        }

        for (String adminEmail : adminEmails) {
            createAdminUser(adminEmail, tenantId);
        }
    }

    public static class MagicAuthenticationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

        private String headerValue;

        public MagicAuthenticationHeaderHttpRequestInterceptor(String headerValue) {
            this.headerValue = headerValue;
        }

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
                throws IOException {
            HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
            requestWrapper.getHeaders().add(Constants.INTERNAL_SERVICE_HEADERNAME, headerValue);

            return execution.execute(requestWrapper, body);
        }
    }

    private void createAdminUser(String username, String tenantId) {
        // construct User
        User adminUser = new User();
        adminUser.setUsername(username);
        adminUser.setFirstName("Super");
        adminUser.setLastName("Admin");
        adminUser.setAccessLevel(AccessLevel.SUPER_ADMIN.name());
        adminUser.setActive(true);
        adminUser.setTitle("Lattice PLO");
        adminUser.setEmail(username);

        // construct credential
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword("EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");

        // construct user registration
        UserRegistration uReg = new UserRegistration();
        uReg.setUser(adminUser);
        uReg.setCredentials(creds);

        UserRegistrationWithTenant urt = new UserRegistrationWithTenant();
        urt.setUserRegistration(uReg);
        urt.setTenant(tenantId);

        boolean adminCreated = restTemplate.postForObject(RESTAPI_HOST_PORT + "/pls/admin/users", urt, Boolean.class);
        if (!adminCreated) {
            throw new LedpException(
                    LedpCode.LEDP_18028, "Failed to create the admin user " + username, new RuntimeException());
        }
    }

}
