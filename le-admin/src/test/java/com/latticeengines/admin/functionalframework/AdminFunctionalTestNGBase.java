package com.latticeengines.admin.functionalframework;

import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
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

import com.latticeengines.camille.exposed.config.bootstrap.ServiceWarden;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.admin.TenantRegistration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.DocumentDirectory;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceServiceScope;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.security.exposed.Constants;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class AdminFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(AdminFunctionalTestNGBase.class);

    @Value("${admin.adtester.username}")
    protected String ADTesterUsername;

    @Value("${admin.adtester.password}")
    protected String ADTesterPassword;

    @Value("${admin.api.hostport}")
    private String hostPort;

    @Autowired
    private TestLatticeComponent testLatticeComponent;

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
        loginAD();
        testLatticeComponent.register();
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

        TenantRegistration reg = new TenantRegistration();
        reg.setSpaceInfo(info);
        reg.setContractInfo(new ContractInfo(new ContractProperties()));
        reg.setTenantInfo(new TenantInfo(new TenantProperties(info.properties.displayName, info.properties.description)));

        String url = String.format("%s/admin/tenants/%s?contractId=%s",getRestHostPort(), tenantId, contractId);
        Boolean created = restTemplate.postForObject(url, reg, Boolean.class);
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


    protected static Map<String, String> flattenPropConfig(CustomerSpaceProperties spaceProp, DocumentDirectory configDir) {
        Map<String, String> toReturn = new HashMap<>();
        Iterator<DocumentDirectory.Node> iter = configDir.breadthFirstIterator();
        while (iter.hasNext()) {
            DocumentDirectory.Node node = iter.next();
            if (node.getDocument() != null && !node.getDocument().getData().equals("")) {
                toReturn.put(node.getPath().toString(), node.getDocument().getData());
            }
        }
        if (spaceProp != null) {
            toReturn.put("CustomerSpaceProperties", JsonUtils.serialize(spaceProp));
        }
        return toReturn;
    }

}
