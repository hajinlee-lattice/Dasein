package com.latticeengines.testframework.security.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.message.BasicNameValuePair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.HttpClientWithOptionalRetryUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.testframework.security.GlobalAuthTestBed;

public class GlobalAuthDeploymentTestBed extends AbstractGlobalAuthTestBed implements GlobalAuthTestBed {

    private static final Log log = LogFactory.getLog(GlobalAuthDeploymentTestBed.class);
    private static final String testTenantRegJson = "{product}-tenant-registration-{env}.json";
    private static final String sfdcTopology = CRMTopology.SFDC.getName();
    private static final List<String> testCustomerSpaces = new ArrayList<>();

    public static final String ENV_QA = "qa";
    public static final String ENV_PROD = "prod";

    private String enviroment;
    private String plsApiHostPort;
    private String adminApiHostPort;
    private Boolean involvedDL;
    private Boolean involvedZK;

    @Autowired
    private DataLoaderService dataLoaderService;

    public GlobalAuthDeploymentTestBed(String plsApiHostPort, String adminApiHost, String environment) {
        super();
        setPlsApiHostPort(plsApiHostPort);
        setAdminApiHostPort(adminApiHost);
        if (environment.contains("prod")) {
            this.enviroment = ENV_PROD;
        } else {
            this.enviroment = ENV_QA;
        }
    }

    protected void setPlsApiHostPort(String plsApiHostPort) {
        if (plsApiHostPort.endsWith("/")) {
            plsApiHostPort = plsApiHostPort.substring(0, plsApiHostPort.lastIndexOf("/"));
        }
        this.plsApiHostPort = plsApiHostPort;
    }

    protected void setAdminApiHostPort(String adminApiHostPort) {
        if (adminApiHostPort.endsWith("/")) {
            adminApiHostPort = adminApiHostPort.substring(0, adminApiHostPort.lastIndexOf("/"));
        }
        this.adminApiHostPort = adminApiHostPort;
    }

    @Override
    public void bootstrap(Integer numTenants) {
        involvedDL = false;
        involvedZK = false;
        super.bootstrap(numTenants);
    }

    @Override
    public void bootstrapForProduct(LatticeProduct product) {
        bootstrapViaTenantConsole(product, enviroment);
        involvedDL = (LatticeProduct.LPA.equals(product));
        involvedZK = false;
    }

    @Override
    public void cleanup() {
        super.cleanup();
        if (involvedZK) {
            cleanupTenantsInZK();
        }
        if (involvedDL) {
            cleanupTenantsInDL();
        }
    }

    @Override
    public UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(plsApiHostPort + "/pls/login", creds, LoginDocument.class);

        authHeaderInterceptor.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authHeaderInterceptor }));

        UserDocument userDocument = restTemplate.postForObject(plsApiHostPort + "/pls/attach", tenant, UserDocument.class);
        log.info("Log in user " + username + " to tenant " + tenant.getId() + " through REST call.");
        return userDocument;
    }

    @Override
    protected void logout(UserDocument userDocument) {
        useSessionDoc(userDocument);
        restTemplate.getForObject(plsApiHostPort + "/pls/logout", Object.class);
    }

    @Override
    protected void bootstrapUser(AccessLevel accessLevel, Tenant tenant) {
        String username = TestFrameworkUtils.usernameForAccessLevel(accessLevel);

        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        userRegistrationWithTenant.setTenant(tenant.getId());
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        User user = new User();
        user.setActive(true);
        user.setEmail(username);
        user.setFirstName(TestFrameworkUtils.TESTING_USER_FIRST_NAME);
        user.setLastName(TestFrameworkUtils.TESTING_USER_LAST_NAME);
        user.setUsername(username);
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(TestFrameworkUtils.GENERAL_PASSWORD_HASH);
        userRegistration.setUser(user);
        userRegistration.setCredentials(creds);
        Boolean success = magicRestTemplate.postForObject(plsApiHostPort + "/pls/admin/users", userRegistrationWithTenant, Boolean.class);
        log.info("Create admin user " + username + ": success=" + success);

        if (!AccessLevel.SUPER_ADMIN.equals(accessLevel)) {
            UserUpdateData userUpdateData = new UserUpdateData();
            userUpdateData.setAccessLevel(accessLevel.name());
            switchToSuperAdmin(tenant);
            restTemplate.put(plsApiHostPort + "/pls/users/" + username, userUpdateData);
            log.info("Change user " + username + " access level to tenant " + tenant.getId() + " to " + accessLevel);
        }
    }

    @Override
    public void createTenant(Tenant tenant) {
        magicRestTemplate.postForObject(plsApiHostPort + "/pls/admin/tenants", tenant, Boolean.class);
    }

    @Override
    public void deleteTenant(Tenant tenant) {
        deleteTenantViaTenantConsole(tenant);
        magicRestTemplate.delete(plsApiHostPort + "/pls/admin/tenants/" + tenant.getId());
        log.info("DELETE tenant from pls and GA");
    }

    /**
     * bootstrap with one full tenant through tenant console
     */
    public void bootstrapViaTenantConsole(LatticeProduct latticeProduct, String environment) {
        Tenant tenant = addBuiltInTestTenant();
        String jsonFileName = testTenantRegJson.replace("{product}", latticeProduct.name().toLowerCase())
                .replace("{env}", environment);
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        try {
            provisionThroughTenantConsole(customerSpace.toString(), sfdcTopology, jsonFileName);
        } catch (Exception e) {
            throw new RuntimeException("Failed to provision tenant via tenant console.", e);
        }
        testCustomerSpaces.add(customerSpace.toString());
        waitForTenantConsoleInstallation(CustomerSpace.parse(tenant.getId()));
    }

    private void provisionThroughTenantConsole(String tupleId, String topology, String tenantRegJson)
            throws IOException {
        String url = "tenantconsole/" + tenantRegJson;
        log.info("Using tenant registration template " + url);
        List<BasicNameValuePair> adHeaders = loginAd();
        String tenantToken = "${TENANT}";
        String topologyToken = "${TOPOLOGY}";
        String dlTenantName = CustomerSpace.parse(tupleId).getTenantId();
        InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(url);
        if (ins == null) {
            throw new IOException("Cannot find resource [" + url + "]");
        }
        String payload = IOUtils.toString(ins, "UTF-8");
        payload = payload.replace(tenantToken, dlTenantName).replace(topologyToken, topology);
        TenantRegistration tenantRegistration = JsonUtils.deserialize(payload, TenantRegistration.class);
        log.info("Tenant Registration:\n" + JsonUtils.serialize(tenantRegistration));
        HttpClientWithOptionalRetryUtils.sendPostRequest(
                adminApiHostPort + "/admin/tenants/" + dlTenantName + "?contractId=" + dlTenantName, false, adHeaders,
                payload);
    }

    private void waitForTenantConsoleInstallation(CustomerSpace customerSpace) {
        Long timeout = TimeUnit.MINUTES.toMillis(5L);
        long totTime = 0L;
        String url = adminApiHostPort + "/admin/tenants/" + customerSpace.getTenantId() + "?contractId="
                + customerSpace.getContractId();
        BootstrapState state = BootstrapState.createInitialState();
        while (!BootstrapState.State.OK.equals(state.state) && !BootstrapState.State.ERROR.equals(state.state)
                && totTime <= timeout) {
            try {
                List<BasicNameValuePair> adHeaders = loginAd();
                String jsonResponse = HttpClientWithOptionalRetryUtils.sendGetRequest(url, false, adHeaders);
                log.info("JSON response from tenant console: " + jsonResponse);
                TenantDocument tenantDocument = JsonUtils.deserialize(jsonResponse, TenantDocument.class);
                BootstrapState newState = tenantDocument.getBootstrapState();
                state = newState == null ? state : newState;
            } catch (IOException e) {
                throw new RuntimeException("Failed to query tenant installation state", e);
            } finally {
                try {
                    Thread.sleep(5000L);
                    totTime += 5000L;
                } catch (InterruptedException e) {
                    log.error(e);
                }
            }
        }

        if (!BootstrapState.State.OK.equals(state.state)) {
            if (involvedDL) {
                // bardjams may fail, and it won't impact test
                log.warn("The tenant state is not OK after " + timeout + " msec.");
            } else  {
                throw new IllegalArgumentException("The tenant state is not OK after " + timeout + " msec.");
            }
        }
    }

    private void cleanupTenantsInZK() {
        for (Tenant tenant: testTenants) {
            log.info("Clean up test tenant " + tenant.getId() + " from zk.");
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            String contractId = CustomerSpace.parse(tenant.getId()).getContractId();
            Path contractPath = PathBuilder.buildContractPath(podId, contractId);
            try {
                camille.delete(contractPath);
            } catch (Exception e) {
                log.error("Failed delete contract path " + contractPath + " from zk.");
            }
        }
    }

    private void cleanupTenantsInDL() {
        for (Tenant tenant: testTenants) {
            log.info("Clean up test tenant " + tenant.getId() + " from DL.");
            CustomerSpace customerSpace =CustomerSpace.parse(tenant.getId());
            String tenantName = customerSpace.getTenantId();

            try {
                String permStoreUrl = adminApiHostPort + "/admin/internal/BODCDEVVINT207/BODCDEVVINT187/" + tenantName;
                magicRestTemplate.delete(permStoreUrl);
                log.info("Cleanup VDB permstore for tenant " + tenantName);
            } catch (Exception e) {
                log.error("Failed to clean up permstore for vdb " + tenantName + " : "
                        + getErrorHandler().getStatusCode() + ", " + getErrorHandler().getResponseString());
            }

            try {
                List<BasicNameValuePair> adHeaders = loginAd();
                String adminUrl = adminApiHostPort + "/admin/tenants/" + customerSpace.getTenantId() + "?contractId="
                        + customerSpace.getContractId();
                String response = HttpClientWithOptionalRetryUtils.sendGetRequest(adminUrl, false, adHeaders);
                TenantDocument tenantDoc = JsonUtils.deserialize(response, TenantDocument.class);
                String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
                DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenantName, "3");
                InstallResult result = dataLoaderService.deleteDLTenant(request, dlUrl, true);
                log.info("Delete DL tenant " + tenantName + " result=" + JsonUtils.serialize(result));
            } catch (Exception e) {
                log.error("Failed to clean up dl tenant " + tenantName + " : "
                        + getErrorHandler().getStatusCode() + ", " + getErrorHandler().getResponseString());
            }
        }
    }

    private void deleteTenantViaTenantConsole(Tenant tenant) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        if (testCustomerSpaces.contains(customerSpace.toString())) {
            try {
                List<BasicNameValuePair> adHeaders = loginAd();
                String url = adminApiHostPort + "/admin/tenants/" + customerSpace.getTenantId() + "?contractId="
                        + customerSpace.getContractId();
                String jsonResponse = HttpClientWithOptionalRetryUtils.sendGetRequest(url, false, adHeaders);
                log.info("DELETE customer space " + customerSpace + " in tenant console: " + jsonResponse);
            } catch (Exception e) {
                log.warn("DELETE customer space " + customerSpace + " in tenant console failed.", e);
            }
        }
    }

    private List<BasicNameValuePair> loginAd() throws IOException {
        List<BasicNameValuePair> headers = new ArrayList<>();
        headers.add(new BasicNameValuePair("Content-Type", "application/json"));
        headers.add(new BasicNameValuePair("Accept", "application/json"));

        Credentials credentials = new Credentials();
        credentials.setUsername(TestFrameworkUtils.AD_USERNAME);
        credentials.setPassword(TestFrameworkUtils.AD_PASSWORD);
        String response = HttpClientWithOptionalRetryUtils.sendPostRequest(adminApiHostPort + "/admin/adlogin", false,
                headers, JsonUtils.serialize(credentials));

        ObjectMapper mapper = new ObjectMapper();
        JsonNode json = mapper.readTree(response);
        String token = json.get("Token").asText();

        headers.add(new BasicNameValuePair("Authorization", token));
        return headers;
    }

}
