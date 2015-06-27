package com.latticeengines.upgrade.functionalframework;

import java.io.IOException;
import java.util.Arrays;

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

import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.security.exposed.Constants;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-upgrade-context.xml" })
public class UpgradeFunctionalTestNGBase  extends AbstractTestNGSpringContextTests {

    protected static final String CUSTOMER = "Nutanix_PLS132";
    protected static final String TUPLE_ID = "Nutanix_PLS132.Nutanix_PLS132.Production";
    protected static final String MODEL_GUID = "ms__5d074f72-c8f0-4d53-aebc-912fb066daa0-PLSModel";
    protected static final String UUID = "5d074f72-c8f0-4d53-aebc-912fb066daa0";
    protected static final String EVENT_TABLE = "Q_PLS_Modeling_Nutanix_PLS132";
    protected static final String CONTAINER_ID = "1416355548818_20011";

    protected static final String DL_URL = "https://data-pls.lattice-engines.com/Dataloader_PLS";
    protected static final CRMTopology TOPOLOGY = CRMTopology.MARKETO;

    protected static RestTemplate magicRestTemplate = new RestTemplate();
    protected static MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader
            = new MagicAuthenticationHeaderHttpRequestInterceptor("");

    @Value("${dataplatform.customer.basedir}")
    protected String customerBase;

    static {
        // setup magic rest template
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader}));
    }

    private static class MagicAuthenticationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

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
}
