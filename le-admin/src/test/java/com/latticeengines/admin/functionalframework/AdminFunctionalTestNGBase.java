package com.latticeengines.admin.functionalframework;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.IOUtils;
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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.admin.service.TenantService;
import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.Camille;
import com.latticeengines.camille.exposed.CamilleEnvironment;
import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.camille.exposed.paths.PathBuilder;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.exposed.Constants;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class AdminFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(AdminFunctionalTestNGBase.class);

    protected static final String ADTesterUsername = Constants.ACTIVE_DIRECTORY_TESTER_USERNAME;
    protected static final String ADTesterPassword = Constants.ACTIVE_DIRECTORY_TESTER_PASSWORD;

    @Autowired
    private BatonService batonService;

    @Autowired
    private TenantService tenantService;
    
    @Autowired
    private TestLatticeComponent testLatticeComponent;
    
    @Value("${admin.api.hostport}")
    private String hostPort;

    private Camille camille;
    private String podId;

    private String token;
    
    protected RestTemplate restTemplate = new RestTemplate();
    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");
    
    public AdminFunctionalTestNGBase() {}

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

    protected String getRestHostPort() {
        return hostPort;
    }
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        //uploadDefaultConfigs();
        //loginAD();
        createTenant("CONTRACT1", "TENANT1");
        CustomerSpaceServiceScope scope = testLatticeComponent.getScope();
        ServiceWarden.commandBootstrap(scope.getServiceName(), scope.getCustomerSpace(), scope.getProperties());
    }
    
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        testLatticeComponent.tearDown();
    }
    
    protected void bootstrap(String contractId, String tenantId, String serviceName, Map<String, String> properties) {
        String url = String.format("%s/admin/tenants/%s/services/%s?contractId=%s", getRestHostPort(), tenantId, serviceName, contractId);
        restTemplate.put(url, properties, new HashMap<>());
    }
    
    protected void deleteTenant(String contractId, String tenantId) throws Exception {
        String url = String.format("%s/admin/tenants/%s?contractId=%s",getRestHostPort(), tenantId, contractId);
        restTemplate.delete(url, new HashMap<>());
    }
    
    protected void createTenant(String contractId, String tenantId) throws Exception {

        if (ContractLifecycleManager.exists(contractId)) {
            ContractLifecycleManager.delete(contractId);
        }
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = String.format("Test tenant for contract id %s and tenant id %s", contractId, tenantId);
        props.displayName = "Tenant for testing";
        CustomerSpaceInfo info = new CustomerSpaceInfo(props, "");
        System.out.println(JsonUtils.serialize(info));
        log.info(String.format("Creating tenant %s.%s in %s.", contractId, tenantId, CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID));
//        String url = String.format("%s/admin/tenants/%s?contractId=%s",getRestHostPort(), tenantId, contractId);
//        Boolean created = restTemplate.postForObject(url, info, Boolean.class);
//        assertTrue(created);
        // bypass AD temporarily
        tenantService.createTenant(contractId, tenantId, info);
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
            requestWrapper.getHeaders().add("MagicAuthentication", headerValue);

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

        Map<String, String> map = restTemplate.postForObject(getRestHostPort() + "/admin/adlogin", creds, Map.class);
        token = map.get("Token");
        assertNotNull(token);
        addAuthHeader.setAuthValue(token);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
    }

    private void uploadDefaultConfigs() throws Exception {
        uploadDefaultConfig("PLS", "pls_default.json", "pls_metadata.json");
        uploadDefaultConfig("GA", "ga_default.json", "ga_metadata.json");
        uploadDefaultConfig("VDB", "vdb_default.json", "vdb_metadata.json");
    }

    private void uploadDefaultConfig(String componentName, String configFile, String metadataFile) throws Exception {
        String configStr = IOUtils.toString(
                Thread.currentThread().getContextClassLoader().getResourceAsStream(configFile),
                "UTF-8"
        );
        String metaStr = null;
        if (metadataFile != null) {
            metaStr = IOUtils.toString(
                    Thread.currentThread().getContextClassLoader().getResourceAsStream(metadataFile),
                    "UTF-8"
            );
        }
        SerializableDocumentDirectory sDir =  new SerializableDocumentDirectory(configStr, metaStr);
        DocumentDirectory configDir = SerializableDocumentDirectory.deserialize(sDir);
        DocumentDirectory metaDir = sDir.getMetadataAsDirectory();

        camille = CamilleEnvironment.getCamille();
        podId = CamilleEnvironment.getPodId();

        Path defauldConfigPath = PathBuilder.buildServiceDefaultConfigPath(podId, componentName);
        Path metadataPath = PathBuilder.buildServiceConfigSchemaPath(podId, componentName);

        if (camille.exists(defauldConfigPath)) { camille.delete(defauldConfigPath); }
        if (camille.exists(metadataPath)) { camille.delete(metadataPath); }

        batonService.loadDirectory(configDir, defauldConfigPath);
        batonService.loadDirectory(metaDir, metadataPath);
    }

}
