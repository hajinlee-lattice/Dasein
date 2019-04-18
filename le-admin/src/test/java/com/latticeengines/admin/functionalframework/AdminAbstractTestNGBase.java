package com.latticeengines.admin.functionalframework;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpRequest;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.admin.service.impl.TenantServiceImpl.ProductAndExternalAdminInfo;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagDefinition;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;

/**
 * This is the base class of functional tests In BeforeClass, we delete and
 * create one test tenant In AfterClass, we delete the test tenant
 */
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public abstract class AdminAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(AdminAbstractTestNGBase.class);

    private static final String ADTesterUsername = "testuser1";
    private static final String ADTesterPassword = "Lattice1";
    protected static BatonService batonService;
    protected static final FeatureFlagDefinition FLAG_DEFINITION = newFlagDefinition();
    protected static final String FLAG_ID = "TestFlag";

    @Autowired
    private TenantService tenantService;

    @Value("${admin.test.contract}")
    protected String TestContractId;

    @Value("${admin.test.contract}")
    protected String TestTenantId;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected RestTemplate magicRestTemplate = HttpClientUtils.newRestTemplate();
    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    public AdminAbstractTestNGBase() {
    }

    @PostConstruct
    public void initialize() {
        if (batonService == null) {
            batonService = new BatonServiceImpl();
        }
    }

    protected abstract String getRestHostPort();

    protected void bootstrap(String contractId, String tenantId, String serviceName) {
        CustomerSpace space = new CustomerSpace();
        space.setContractId(contractId);
        space.setTenantId(tenantId);
        space.setSpaceId(CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        DocumentDirectory defaultConfig = batonService.getDefaultConfiguration(serviceName);
        bootstrap(contractId, tenantId, serviceName, defaultConfig);
    }

    protected void bootstrap(String contractId, String tenantId, String serviceName, DocumentDirectory configDir) {
        CustomerSpace space = new CustomerSpace();
        space.setContractId(contractId);
        space.setTenantId(tenantId);
        space.setSpaceId(CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);

        SerializableDocumentDirectory sDir = new SerializableDocumentDirectory(configDir);
        Map<String, String> bootstrapProperties = sDir.flatten();

        loginAD();
        String url = String.format("%s/admin/tenants/%s/services/%s?contractId=%s", getRestHostPort(), tenantId,
                serviceName, contractId);
        restTemplate.put(url, bootstrapProperties, new HashMap<>());
    }

    protected void changeStatus(String contractId, String tenantId) {
        TenantInfo info = null;
        try {
            info = TenantLifecycleManager.getInfo(contractId, tenantId);
        } catch (Exception e) {
            e.printStackTrace();
        }
        info.properties.status = "ACTIVE";
        String url = String.format("%s/admin/tenants/%s/%s", getRestHostPort(), tenantId, contractId);
        restTemplate.put(url, info, new HashMap<>());
    }
    protected void deleteTenant(String contractId, String tenantId) throws Exception {
        deleteTenant(contractId, tenantId, true);
    }

    protected void deleteTenant(String contractId, String tenantId, boolean deleteZookeeper) throws Exception {
        log.info(String.format("Begin deleting the tenant %s", tenantId));
        String url = String.format("%s/admin/tenants/%s?contractId=%s&deleteZookeeper=%b", getRestHostPort(), tenantId,
                contractId, deleteZookeeper);
        restTemplate.delete(url, new HashMap<>());
        if (waitUntilTenantDeleted(contractId, tenantId, 200)) {
            log.info(String.format("Successfully deleted the tenant %s", tenantId));
        } else {
            log.warn(String.format("Tenant %s maybe not deleted.", tenantId));
        }
    }

    protected void createTenant(String contractId, String tenantId) throws Exception {
        createTenant(contractId, tenantId, true);
    }

    protected void createTenant(String contractId, String tenantId, boolean refreshContract) throws Exception {
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = String.format("Test tenant for contract id %s and tenant id %s", contractId, tenantId);
        props.displayName = tenantId + ": Tenant for testing";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(new TenantProperties(spaceInfo.properties.displayName,
                spaceInfo.properties.description));

        SpaceConfiguration spaceConfig = tenantService.getDefaultSpaceConfig();

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(spaceInfo);
        reg.setTenantInfo(tenantInfo);
        reg.setContractInfo(contractInfo);
        reg.setSpaceConfig(spaceConfig);

        createTenant(contractId, tenantId, refreshContract, reg);
    }

    protected void createTenant(String contractId, String tenantId, boolean refreshContract,
            TenantRegistration tenantRegistration) throws Exception {

        if (ContractLifecycleManager.exists(contractId)) {
            if (refreshContract) {
                ContractLifecycleManager.delete(contractId);
            }
        }
        ContractLifecycleManager.create(contractId, new ContractInfo(new ContractProperties()));

        Assert.assertTrue(ContractLifecycleManager.exists(contractId));

        String url = String.format("%s/admin/tenants/%s?contractId=%s", getRestHostPort(), tenantId, contractId);
        Boolean created = restTemplate.postForObject(url, tenantRegistration, Boolean.class);
        Assert.assertTrue(created);
    }

    @SuppressWarnings("unchecked")
    protected void loginAD() {
        Credentials creds = new Credentials();
        creds.setUsername(ADTesterUsername);
        creds.setPassword(ADTesterPassword);
        restTemplate.setInterceptors(new ArrayList<>());
        Map<String, String> map = restTemplate.postForObject(getRestHostPort() + "/admin/adlogin", creds, Map.class);
        String token = map.get("Token");
        assertNotNull(token);
        addAuthHeader.setAuthValue(token);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
    }

    public static class AuthorizationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

        private String headerValue;

        public AuthorizationHeaderHttpRequestInterceptor(String headerValue) {
            this.headerValue = headerValue;
        }

        @Override
        public ClientHttpResponse intercept(HttpRequest request, byte[] body, ClientHttpRequestExecution execution)
                throws IOException {
            HttpRequestWrapper requestWrapper = new HttpRequestWrapper(request);
            requestWrapper.getHeaders().add(Constants.AUTHORIZATION, headerValue);

            return execution.execute(requestWrapper, body);
        }

        public void setAuthValue(String headerValue) {
            this.headerValue = headerValue;
        }
    }

    protected BootstrapState waitUntilStateIsNotInitial(String contractId, String tenantId, String serviceName) {
        return waitUntilStateIsNotInitial(contractId, tenantId, serviceName, 100);
    }

    protected BootstrapState waitUntilStateIsNotInitial(String contractId, String tenantId, String serviceName,
            int numOfRetries) {
        BootstrapState state;
        do {
            numOfRetries--;
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Waiting for component state update interrupted", e);
            }
            String url = String.format("%s/admin/tenants/%s/services/%s/state?contractId=%s", getRestHostPort(),
                    tenantId, serviceName, contractId);
            state = restTemplate.getForObject(url, BootstrapState.class);
        } while (state != null && state.state.equals(BootstrapState.State.INITIAL) && numOfRetries > 0);
        return state;
    }

    protected BootstrapState waitUntilStateIsNotUninstalling(String contractId, String tenantId, String serviceName,
                                                        int numOfRetries) {
        String url = String.format("%s/admin/tenants/%s/services/%s/state?contractId=%s", getRestHostPort(),
                tenantId, serviceName, contractId);
        BootstrapState state = restTemplate.getForObject(url, BootstrapState.class);
        while ((state.state.equals(BootstrapState.State.UNINSTALLING) ||
                state.state.equals(BootstrapState.State.INITIAL))  && numOfRetries > 0) {
            numOfRetries--;
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException e) {
                throw new RuntimeException("Waiting for component state update interrupted", e);
            }
            state = tenantService.getTenantServiceState(contractId, tenantId, serviceName);
        }
        return state;
    }

    protected boolean waitUntilTenantDeleted(String contractId, String tenantId, int numOfRetries) {
        while(numOfRetries > 0) {
            numOfRetries--;
            try {
                Thread.sleep(1000L);
                String url = String.format("%s/admin/tenants/%s?contractId=%s", getRestHostPort(),
                        tenantId, contractId);
                TenantDocument doc = restTemplate.getForObject(url, TenantDocument.class);
                if (doc.getTenantInfo() == null) {
                    return true;
                }
            } catch (InterruptedException e) {
                throw new RuntimeException("Waiting for tenant delete interrupted", e);
            } catch (Exception e) {
                return true;
            }
        }
        return false;
    }

    public void clearDatastore(String dataStoreOption, String permStoreOption, String visiDBServerName, String tenant) {
        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        String url = String.format("%s/admin/internal/", getRestHostPort());
        magicRestTemplate.delete(url + "datastore/" + dataStoreOption + "/" + tenant);
        magicRestTemplate.delete(url + "permstore/" + permStoreOption + "/" + visiDBServerName + "/" + tenant);

        Boolean permStoreExists = magicRestTemplate.getForObject(url + "permstore/" + permStoreOption + "/"
                + visiDBServerName + "/" + tenant, Boolean.class);
        Assert.assertFalse(permStoreExists);
    }

    protected static FeatureFlagDefinition newFlagDefinition() {
        FeatureFlagDefinition definition = new FeatureFlagDefinition();
        definition.setDisplayName(FLAG_ID);
        definition.setDocumentation("This flag is for functional test.");
        Set<LatticeProduct> testProdSet = new HashSet<LatticeProduct>();
        testProdSet.add(LatticeProduct.LPA);
        testProdSet.add(LatticeProduct.LPA3);
        testProdSet.add(LatticeProduct.PD);
        testProdSet.add(LatticeProduct.CG);
        definition.setAvailableProducts(testProdSet);
        definition.setConfigurable(true);
        return definition;
    }

    protected void defineFeatureFlagByRestCall(String flagId, FeatureFlagDefinition definition) {
        undefineFeatureFlagByRestCall(FLAG_ID);

        String url = getRestHostPort() + "/admin/featureflags/" + flagId;
        ResponseDocument<?> response = restTemplate.postForObject(url, definition, ResponseDocument.class);
        Assert.assertTrue(response.isSuccess(), "should be able to define a new flag.");
    }

    protected void undefineFeatureFlagByRestCall(String flagId) {
        String url = getRestHostPort() + "/admin/featureflags/" + flagId;
        restTemplate.delete(url);
    }

    public ProductAndExternalAdminInfo generateLPAandEmptyExternalAdminInfo() {
        ProductAndExternalAdminInfo prodAndExternalAminInfo = new ProductAndExternalAdminInfo();
        List<LatticeProduct> products = Arrays.asList(LatticeProduct.LPA);
        Map<String, Boolean> externalEmailMap = new HashMap<String, Boolean>();
        prodAndExternalAminInfo.setExternalEmailMap(externalEmailMap);
        prodAndExternalAminInfo.setProducts(products);
        return prodAndExternalAminInfo;
    }

    public ProductAndExternalAdminInfo generateProductAndExternalAdminInfo() {
        ProductAndExternalAdminInfo prodAndExternalAminInfo = new ProductAndExternalAdminInfo();
        List<LatticeProduct> products = new ArrayList<LatticeProduct>();
        products.add(LatticeProduct.LPA);
        products.add(LatticeProduct.PD);
        Map<String, Boolean> externalEmailMap = new HashMap<String, Boolean>();
        externalEmailMap.put("michael@fake.com", false);
        externalEmailMap.put("jane@fake.com", false);
        externalEmailMap.put("lucas@fake.com", true);
        prodAndExternalAminInfo.setExternalEmailMap(externalEmailMap);
        prodAndExternalAminInfo.setProducts(products);
        return prodAndExternalAminInfo;
    }
}
