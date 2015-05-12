package com.latticeengines.admin.functionalframework;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.baton.exposed.service.impl.BatonServiceImpl;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.lifecycle.TenantLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.admin.TenantDocument;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.Document;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.exposed.Constants;

/**
 * This is the base class of functional tests
 * In BeforeClass, we delete and create one test tenant
 * In AfterClass, we delete the test tenant
 */
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class AdminFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(AdminFunctionalTestNGBase.class);

    protected static final String ADTesterUsername = "testuser1";
    protected static final String ADTesterPassword = "Lattice1";
    protected static final String TestTenantId = "TestTenant";
    protected static final BatonService batonService = new BatonServiceImpl();
    private static boolean ZKIsClean = false;

    @Value("${admin.test.contract}")
    protected String TestContractId;

    @Value("${admin.api.hostport}")
    private String hostPort;

    @Autowired
    private TenantService tenantService;

    protected RestTemplate restTemplate = new RestTemplate();
    protected RestTemplate magicRestTemplate = new RestTemplate();
    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");
    
    public AdminFunctionalTestNGBase() {}

    protected String getRestHostPort() {
        return hostPort;
    }

    protected void cleanupZK() {
        if (ZKIsClean) return;

        log.info("Checking the sanity of contracts and tenants in ZK.");

        boolean ZKHasIssues = false;

        try {
            for (TenantDocument tenantDocument: batonService.getTenants(null)) {
                if (tenantDocument.getContractInfo() == null ||
                        tenantDocument.getTenantInfo() == null ||
                        tenantDocument.getSpaceInfo() == null ||
                        tenantDocument.getSpaceConfig() == null) {
                    ZKHasIssues = true;
                    break;
                }
            }
        } catch (Exception e) {
            ZKHasIssues = true;
        }

        if (ZKHasIssues) {
            log.info("Cleaning up bad contracts and tenants in ZK");
            Camille camille = CamilleEnvironment.getCamille();
            String podId = CamilleEnvironment.getPodId();
            boolean contractsExist = false;
            try {
                contractsExist = camille.exists(PathBuilder.buildContractsPath(podId));
            } catch (Exception e) {
                log.warn("Getting Contracts node error.");
            }

            if (contractsExist) {
                List<AbstractMap.SimpleEntry<Document, Path>> contractDocs = new ArrayList<>();
                try {
                    contractDocs = camille.getChildren(PathBuilder.buildContractsPath(podId));
                } catch (Exception e) {
                    log.warn("Getting Contract Documents error.");
                }

                for (AbstractMap.SimpleEntry<Document, Path> entry: contractDocs) {
                    String contractId = entry.getValue().getSuffix();
                    ContractInfo contractInfo = null;
                    try {
                        contractInfo = ContractLifecycleManager.getInfo(contractId);
                    } catch (Exception e) {
                        log.warn("Found a bad contract: " + contractId + ". Deleting it ...");
                        try {
                            ContractLifecycleManager.delete(contractId);
                        } catch (Exception e2) {
                            // ignore
                        }
                    }

                    if (contractInfo != null) {
                        boolean tenantsExist = false;
                        try {
                            tenantsExist = camille.exists(PathBuilder.buildTenantsPath(podId, contractId));
                        } catch (Exception e) {
                            log.warn(String.format("Getting Tenants node for contract %s error.", contractId));
                        }
                        if (tenantsExist) {
                            List<AbstractMap.SimpleEntry<Document, Path>> tenantDocs = new ArrayList<>();
                            try {
                                tenantDocs = camille.getChildren(PathBuilder.buildTenantsPath(podId, contractId));
                            } catch (Exception e) {
                                log.warn(String.format(
                                        "Getting Tenant Documents for contract %s error.", contractId));
                            }

                            for (AbstractMap.SimpleEntry<Document, Path> tenantEntry: tenantDocs) {
                                String tenantId = tenantEntry.getValue().getSuffix();

                                try {
                                    TenantDocument tenantDocument = batonService.getTenant(contractId, tenantId);
                                    if (tenantDocument.getContractInfo() == null ||
                                            tenantDocument.getTenantInfo() == null ||
                                            tenantDocument.getSpaceInfo() == null ||
                                            tenantDocument.getSpaceConfig() == null) {
                                        throw new Exception("Tenant: " + contractId + "-"
                                                + tenantId + " does not have a fully valid TenantDocument.");
                                    }
                                } catch (Exception e) {
                                    log.warn("Found a bad tenant: " + contractId + "-" + tenantId + ". Deleting it ...");
                                    try {
                                        TenantLifecycleManager.delete(contractId, tenantId);
                                    } catch (Exception e2) {
                                        // ignore
                                    }
                                }

                            }
                        }
                    }
                }

            }
        }

        ZKHasIssues = false;
        try {
            for (TenantDocument tenantDocument: batonService.getTenants(null)) {
                Assert.assertNotNull(tenantDocument.getContractInfo());
                Assert.assertNotNull(tenantDocument.getTenantInfo());
                Assert.assertNotNull(tenantDocument.getSpaceInfo());
                Assert.assertNotNull(tenantDocument.getSpaceConfig());
            }
        } catch (Exception e) {
            ZKHasIssues = true;
        }

        Assert.assertFalse(ZKHasIssues);

        ZKIsClean = true;
    }

    @BeforeClass(groups = {"functional", "deployment"})
    public void setup() throws Exception {
        cleanupZK();
        loginAD();

        String podId = CamilleEnvironment.getPodId();
        Assert.assertNotNull(podId);

        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            //ignore
        }
        createTenant(TestContractId, TestTenantId);

        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
    }
    
    @AfterClass(groups = {"functional", "deployment"})
    public void tearDown() throws Exception {
        try {
            deleteTenant(TestContractId, TestTenantId);
        } catch (Exception e) {
            //ignore
        }
    }
    
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
        String url = String.format("%s/admin/tenants/%s/services/%s?contractId=%s",
                getRestHostPort(), tenantId, serviceName, contractId);
        restTemplate.put(url, bootstrapProperties, new HashMap<>());
    }
    
    protected void deleteTenant(String contractId, String tenantId) throws Exception {
        String url = String.format("%s/admin/tenants/%s?contractId=%s",getRestHostPort(), tenantId, contractId);
        restTemplate.delete(url, new HashMap<>());
    }

    protected void createTenant(
            String contractId, String tenantId) throws Exception {
        createTenant(contractId, tenantId, true);
    }

    protected void createTenant(
            String contractId, String tenantId, boolean refreshContract) throws Exception {
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = String.format("Test tenant for contract id %s and tenant id %s", contractId, tenantId);
        props.displayName = "Tenant for testing";
        CustomerSpaceInfo spaceInfo = new CustomerSpaceInfo(props, "");

        ContractInfo contractInfo = new ContractInfo(new ContractProperties());
        TenantInfo tenantInfo = new TenantInfo(
                new TenantProperties(spaceInfo.properties.displayName, spaceInfo.properties.description));

        SpaceConfiguration spaceConfig = tenantService.getDefaultSpaceConfig();

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(spaceInfo);
        reg.setTenantInfo(tenantInfo);
        reg.setContractInfo(contractInfo);
        reg.setSpaceConfig(spaceConfig);

        createTenant(contractId, tenantId, refreshContract, reg);
    }

    protected void createTenant (
            String contractId, String tenantId,
            boolean refreshContract, TenantRegistration tenantRegistration)
            throws Exception
    {

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

        public void setAuthValue(String headerValue) {
            this.headerValue = headerValue;
        }
    }

    @SuppressWarnings("unchecked")
    protected void loginAD(){
        Credentials creds = new Credentials();
        creds.setUsername(ADTesterUsername);
        creds.setPassword(ADTesterPassword);
        restTemplate.setInterceptors(new ArrayList<ClientHttpRequestInterceptor>());
        Map<String, String> map = restTemplate.postForObject(getRestHostPort() + "/admin/adlogin", creds, Map.class);
        String token = map.get("Token");
        assertNotNull(token);
        addAuthHeader.setAuthValue(token);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
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

}
