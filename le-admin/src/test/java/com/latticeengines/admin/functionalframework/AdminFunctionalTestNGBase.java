package com.latticeengines.admin.functionalframework;

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

import com.latticeengines.baton.exposed.service.BatonService;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceInfo;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-admin-context.xml" })
public class AdminFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(AdminFunctionalTestNGBase.class);
    
    @Autowired
    private BatonService batonService;
    
    @Value("${admin.api.hostport}")
    private String hostPort;

    protected RestTemplate restTemplate = new RestTemplate();
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected String getRestHostPort() {
        return hostPort;
    }
    
    
    protected void createTenant() throws Exception {
        CustomerSpaceProperties props = new CustomerSpaceProperties();
        props.description = "Test tenant";
        props.displayName = "Tenant for testing";
        CustomerSpaceInfo info = new CustomerSpaceInfo(props, "");
        
        log.info(String.format("Creating tenant %s.%s in %s.", "CONTRACT1", "TENANT1", CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID));
        batonService.createTenant("CONTRACT1", "TENANT1", CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID, info);
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
