package com.latticeengines.admin.functionalframework;

import static org.testng.Assert.assertTrue;

import java.io.IOException;

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

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.camille.exposed.config.bootstrap.CustomerSpaceServiceBootstrapManager;
import com.latticeengines.camille.exposed.lifecycle.ContractLifecycleManager;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class AdminFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(AdminFunctionalTestNGBase.class);
    
    @Autowired
    private BatonService batonService;
    
    @Autowired
    private TestLatticeComponent testLatticeComponent;
    
    @Value("${admin.api.hostport}")
    private String hostPort;
    
    protected RestTemplate restTemplate = new RestTemplate();
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");
    
    public AdminFunctionalTestNGBase() {
    }
    
    protected String getRestHostPort() {
        return hostPort;
    }
    
    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        createTenant();
        CustomerSpaceServiceBootstrapManager.bootstrap(testLatticeComponent.getScope());
    }
    
    @AfterClass(groups = "functional")
    public void tearDown() throws Exception {
        testLatticeComponent.tearDown();
    }
    
    protected void createTenant() throws Exception {
        if (ContractLifecycleManager.exists("CONTRACT1")) {
            ContractLifecycleManager.delete("CONTRACT1");
        }
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "Test tenant";
        props.displayName = "Tenant for testing";
        CustomerSpaceInfo info = new CustomerSpaceInfo(props, "");
        System.out.println(JsonUtils.serialize(info));
        log.info(String.format("Creating tenant %s.%s in %s.", "CONTRACT1", "TENANT1", CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID));
        String url = getRestHostPort() + "/admin/tenants/TENANT1?contractId=CONTRACT1";
        Boolean created = restTemplate.postForObject(url, info, Boolean.class);
        assertTrue(created);
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

}
