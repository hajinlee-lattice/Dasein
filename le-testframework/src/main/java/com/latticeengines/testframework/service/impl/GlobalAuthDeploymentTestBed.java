package com.latticeengines.testframework.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.client.ClientHttpRequestInterceptor;

import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.featureflags.FeatureFlagClient;
import com.latticeengines.camille.exposed.lifecycle.SpaceLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.admin.DeleteVisiDBDLRequest;
import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.dataloader.InstallResult;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.pls.UserUpdateData;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.proxy.exposed.ProtectedRestApiProxy;
import com.latticeengines.remote.exposed.service.DataLoaderService;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.testframework.exposed.proxy.admin.AdminInternalProxy;
import com.latticeengines.testframework.exposed.proxy.admin.AdminTenantProxy;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

@SuppressWarnings("deprecation")
public class GlobalAuthDeploymentTestBed extends AbstractGlobalAuthTestBed implements GlobalAuthTestBed {

    private static final Logger log = LoggerFactory.getLogger(GlobalAuthDeploymentTestBed.class);
    private static final String testTenantRegJson = "{product}-tenant-registration-{env}.json";
    private static final String sfdcTopology = CRMTopology.SFDC.getName();
    private static final List<String> testCustomerSpaces = new ArrayList<>();

    public static final String ENV_QA = "qa";
    public static final String ENV_PROD = "prod";

    private String enviroment;
    private String plsApiHostPort;
    @SuppressWarnings("unused")
    private String adminApiHostPort;
    private boolean involvedDL = false;
    private boolean involvedZK = false;

    @Autowired
    private DataLoaderService dataLoaderService;

    @Autowired
    private AdminTenantProxy adminTenantProxy;

    @Autowired
    private AdminInternalProxy adminInternalProxy;

    @Autowired
    private TenantService tenantService;

    public GlobalAuthDeploymentTestBed(String plsApiHostPort, String adminApiHostPort, String environment) {
        super();
        setPlsApiHostPort(plsApiHostPort);
        setAdminApiHostPort(adminApiHostPort);
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
        loginAD();
        involvedDL = false;
        involvedZK = false;
        super.bootstrap(numTenants);
    }

    @Override
    public void overwriteFeatureFlag(Tenant tenant, String featureFlagName, boolean value) {
        log.info(String.format("Overwriting the feature flag for %s to be %s", featureFlagName, String.valueOf(value)));
        CustomerSpace space = CustomerSpace.parse(tenant.getId());
        FeatureFlagClient.setEnabled(space, featureFlagName, value);
        assert (FeatureFlagClient.getFlags(space).get(featureFlagName) == value);
    }

    @Override
    public Tenant bootstrapForProduct(LatticeProduct product) {
        loginAD();
        Tenant tenant = bootstrapViaTenantConsole(product, enviroment, null);
        involvedDL = (LatticeProduct.LPA.equals(product));
        involvedZK = false;
        return tenant;
    }

    @Override
    public Tenant bootstrapForProduct(String tenantIdentifier, LatticeProduct product) {
        loginAD();
        Tenant tenant = bootstrapViaTenantConsole(tenantIdentifier, product, enviroment, null);
        involvedDL = (LatticeProduct.LPA.equals(product));
        involvedZK = false;
        return tenant;
    }

    @Override
    public Tenant bootstrapForProduct(LatticeProduct product, Map<String, Boolean> featureFlagMap) {
        loginAD();
        Tenant tenant = bootstrapViaTenantConsole(product, enviroment, featureFlagMap);
        involvedDL = (LatticeProduct.LPA.equals(product));
        involvedZK = false;
        return tenant;
    }

    @Override
    public Tenant bootstrapForProduct(String tenantIdentifier, LatticeProduct product,
            Map<String, Boolean> featureFlagMap) {
        loginAD();
        Tenant tenant = bootstrapViaTenantConsole(product, enviroment, featureFlagMap);
        involvedDL = (LatticeProduct.LPA.equals(product));
        involvedZK = false;
        return tenant;
    }

    @Override
    public Tenant bootstrapForProduct(LatticeProduct product, String jsonFileName) {
        loginAD();
        Tenant tenant = bootstrapViaTenantConsoleByFileName(jsonFileName);
        involvedDL = (LatticeProduct.LPA.equals(product));
        involvedZK = false;
        return tenant;
    }

    @Override
    public void cleanupDlZk() {
        if (involvedZK) {
            cleanupTenantsInZK();
        }
        if (involvedDL) {
            cleanupTenantsInDL();
        }
    }

    @Override
    public void cleanupPlsHdfs() {
        loginAD();
        super.cleanupPlsHdfs();
    }

    @Override
    public UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(plsApiHostPort + "/pls/login", creds, LoginDocument.class);

        authHeaderInterceptor.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { authHeaderInterceptor }));

        UserDocument userDocument = restTemplate.postForObject(plsApiHostPort + "/pls/attach", tenant,
                UserDocument.class);
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
        Boolean success = magicRestTemplate.postForObject(plsApiHostPort + "/pls/admin/users",
                userRegistrationWithTenant, Boolean.class);
        log.info("Create admin user " + username + ": success=" + success);

        if (!AccessLevel.SUPER_ADMIN.equals(accessLevel)) {
            UserUpdateData userUpdateData = new UserUpdateData();
            userUpdateData.setAccessLevel(accessLevel.name());
            magicRestTemplate.put(
                    plsApiHostPort + "/pls/admin/users?username=" + username + "&tenant=" + tenant.getId(),
                    userUpdateData, new HashMap<String, Object>());
            log.info("Change user " + username + " access level to tenant " + tenant.getId() + " to " + accessLevel);
        }
    }

    @Override
    public void createTenant(Tenant tenant) {
        log.info("plsApiHostPort is " + plsApiHostPort);
        magicRestTemplate.postForObject(plsApiHostPort + "/pls/admin/tenants", tenant, Boolean.class);
    }

    @Override
    public void deleteTenant(Tenant tenant) {
        boolean needToWait = deleteTenantViaTenantConsole(tenant);
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        log.info("Deleting test tenant " + tenant.getId() + " from pls and GA");
        if (needToWait) {
            waitForTenantConsoleUninstall(customerSpace);
        }
        if (testCustomerSpaces.contains(customerSpace.toString())) {
            testCustomerSpaces.remove(customerSpace.toString());
        }
    }

    private void waitForTenantConsoleUninstall(CustomerSpace customerSpace) {
        Long timeout = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10L);
        while (System.currentTimeMillis() < timeout) {
            try {
                if (!SpaceLifecycleManager.exists(customerSpace.getContractId(), customerSpace.getTenantId(),
                        customerSpace.getSpaceId())) {
                    log.info("CustomSpace " + customerSpace + " is removed from in ZK.");
                    return;
                } else {
                    log.info("CustomSpace " + customerSpace + " is still in ZK.");
                    Thread.sleep(1000);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        throw new IllegalStateException("Tenant console uninstall did not finish with in 10 min.");
    }

    /**
     * bootstrap with one full tenant through tenant console
     */
    private Tenant bootstrapViaTenantConsole(LatticeProduct latticeProduct, String environment,
            Map<String, Boolean> featureFlagMap) {
        Tenant tenant = addBuiltInTestTenant();
        return bootstrapViaTenantConsole(tenant, latticeProduct, environment, featureFlagMap);
    }

    private Tenant bootstrapViaTenantConsole(String tenantIdentifier, LatticeProduct latticeProduct, String environment,
            Map<String, Boolean> featureFlagMap) {
        Tenant tenant = addTestTenant(tenantIdentifier);
        return bootstrapViaTenantConsole(tenant, latticeProduct, environment, featureFlagMap);
    }

    private Tenant bootstrapViaTenantConsole(Tenant tenant, LatticeProduct latticeProduct, String environment,
            Map<String, Boolean> featureFlagMap) {
        String jsonFileName = testTenantRegJson.replace("{product}", latticeProduct.name().toLowerCase())
                .replace("{env}", environment);
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        try {
            provisionThroughTenantConsole(customerSpace.toString(), sfdcTopology, jsonFileName, featureFlagMap);
        } catch (Exception e) {
            throw new RuntimeException("Failed to provision tenant via tenant console.", e);
        }
        testCustomerSpaces.add(customerSpace.toString());
        waitForTenantConsoleInstallation(CustomerSpace.parse(tenant.getId()));

        if (featureFlagMap != null) {
            log.info("Overwriting featureFlags " + featureFlagMap);
            for (String featureFlagId : featureFlagMap.keySet()) {
                overwriteFeatureFlag(tenant, featureFlagId, featureFlagMap.get(featureFlagId));
            }
        }
        return tenant;
    }

    private Tenant bootstrapViaTenantConsoleByFileName(String jsonFileName) {
        Tenant tenant = addBuiltInTestTenant();
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        try {
            provisionThroughTenantConsole(customerSpace.toString(), sfdcTopology, jsonFileName, null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to provision tenant via tenant console.", e);
        }
        testCustomerSpaces.add(customerSpace.toString());
        waitForTenantConsoleInstallation(CustomerSpace.parse(tenant.getId()));
        return tenant;
    }

    private void provisionThroughTenantConsole(String tupleId, String topology, String tenantRegJson,
            Map<String, Boolean> featureFlagMap) throws IOException {
        String url = "tenantconsole/" + tenantRegJson;
        log.info("Using tenant registration template " + url);
        String tenantToken = "${TENANT}";
        String topologyToken = "${TOPOLOGY}";
        String dlTenantName = CustomerSpace.parse(tupleId).getTenantId();
        InputStream ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(url);
        if (ins == null) {
            throw new IOException("Cannot find resource [" + url + "]");
        }
        String payload = null;
        if (featureFlagMap != null && featureFlagMap.containsKey(LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION.getName())
                && !featureFlagMap.get(LatticeFeatureFlag.ENABLE_DATA_ENCRYPTION.getName())) {
            payload = overWriteEncrptionFeatureFlagToFalse(ins);
        } else {
            payload = IOUtils.toString(ins, "UTF-8");
        }
        payload = payload.replace(tenantToken, dlTenantName).replace(topologyToken, topology);
        TenantRegistration tenantRegistration = JsonUtils.deserialize(payload, TenantRegistration.class);
        log.info("Tenant Registration:\n" + JsonUtils.serialize(tenantRegistration));
        adminTenantProxy.createTenant(dlTenantName, tenantRegistration);
    }

    private String overWriteEncrptionFeatureFlagToFalse(InputStream ins) throws IOException {
        String input = IOUtils.toString(ins, "UTF-8");
        Pattern pattern = Pattern.compile("(\\\\\"EnableDataEncryption\\\\\")(:true)");
        Matcher matcher = pattern.matcher(input);
        StringBuffer result = new StringBuffer();
        if (matcher.find()) {
            matcher.appendReplacement(result, "\\\\\"EnableDataEncryption\\\\\":false");
        }
        matcher.appendTail(result);
        return result.toString();
    }

    private void waitForTenantConsoleInstallation(CustomerSpace customerSpace) {
        Long timeout = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(10L);
        BootstrapState state = BootstrapState.createInitialState();
        while (!BootstrapState.State.OK.equals(state.state) && !BootstrapState.State.ERROR.equals(state.state)
                && System.currentTimeMillis() <= timeout) {
            try {
                TenantDocument tenantDoc = adminTenantProxy.getTenant(customerSpace.getTenantId());
                BootstrapState newState = tenantDoc.getBootstrapState();
                log.info("BootstrapState from tenant console: " + (newState == null ? null : newState.state));
                state = newState == null ? state : newState;
                if (BootstrapState.State.OK.equals(state.state) || BootstrapState.State.ERROR.equals(state.state)) {
                    return;
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to query tenant installation state", e);
            } finally {
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        if (!BootstrapState.State.OK.equals(state.state)) {
            if (involvedDL) {
                // bardjams may fail, and it won't impact test
                log.warn("The tenant state is not OK after " + timeout + " msec.");
            } else {
                throw new IllegalArgumentException("The tenant state is not OK after " + timeout + " msec.");
            }
        }
    }

    private void cleanupTenantsInZK() {
        for (Tenant tenant : testTenants) {
            if (excludedCleanupTenantIds.contains(tenant.getId())) {
                log.info("Skip cleaning up " + tenant.getId());
            } else {
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
    }

    private void cleanupTenantsInDL() {
        for (Tenant tenant : testTenants) {
            if (excludedCleanupTenantIds.contains(tenant.getId())) {
                log.info("Skip cleaning up " + tenant.getId());
            } else {
                log.info("Clean up test tenant " + tenant.getId() + " from DL.");
                CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
                String tenantName = customerSpace.getTenantId();

                try {
                    adminInternalProxy.deletePermStore(tenantName);
                    log.info("Cleanup VDB permstore for tenant " + tenantName);
                } catch (Exception e) {
                    log.error("Failed to clean up permstore for vdb " + tenantName + " : "
                            + getErrorHandler().getStatusCode() + ", " + getErrorHandler().getResponseString());
                }

                try {
                    TenantDocument tenantDoc = adminTenantProxy.getTenant(customerSpace.getTenantId());
                    String dlUrl = tenantDoc.getSpaceConfig().getDlAddress();
                    DeleteVisiDBDLRequest request = new DeleteVisiDBDLRequest(tenantName, "3");
                    InstallResult result = dataLoaderService.deleteDLTenant(request, dlUrl, true);
                    log.info("Delete DL tenant " + tenantName + " result=" + JsonUtils.serialize(result));
                } catch (Exception e) {
                    log.error("Failed to clean up dl tenant " + tenantName + " : " + getErrorHandler().getStatusCode()
                            + ", " + getErrorHandler().getResponseString());
                }
            }
        }
    }

    private boolean deleteTenantViaTenantConsole(Tenant tenant) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenant.getId());
        if (testCustomerSpaces.contains(customerSpace.toString())) {
            try {
                adminTenantProxy.deleteTenant(customerSpace.getTenantId());
                return true;
            } catch (Exception e) {
                log.error("DELETE customer space " + customerSpace + " in tenant console failed.", e);
            }
        } else {
            log.warn("Did not find " + customerSpace.toString() + " in testCustomerSpaces.");
        }
        return false;
    }

    @Override
    public void loginAD() {
        adminTenantProxy.login(TestFrameworkUtils.AD_USERNAME, TestFrameworkUtils.AD_PASSWORD);
    }

    public void attachProtectedProxy(ProtectedRestApiProxy proxy) {
        proxy.attachInterceptor(authHeaderInterceptor);
        log.info("Attached pls auth interceptor to " + proxy.getClass().getSimpleName());
    }

    @Override
    public Tenant getTenantBasedOnId(String tenantId) {
        return tenantService.findByTenantId(tenantId);
    }

    @Override
    public FeatureFlagValueMap getFeatureFlags() {
        FeatureFlagValueMap map = getRestTemplate().getForObject(
                plsApiHostPort + "/pls/tenant/featureflags", FeatureFlagValueMap.class);
        
        return map;
    }

}
