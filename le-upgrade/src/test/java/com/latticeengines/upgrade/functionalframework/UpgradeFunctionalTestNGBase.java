package com.latticeengines.upgrade.functionalframework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.latticeengines.domain.exposed.admin.CRMTopology;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.upgrade.pls.PlsGaManager;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-upgrade-context.xml" })
public class UpgradeFunctionalTestNGBase  extends AbstractTestNGSpringContextTests {

    protected static final String CUSTOMER = "Nutanix_PLS132";
    protected static final String TUPLE_ID = "Nutanix_PLS132.Nutanix_PLS132.Production";
    protected static final String MODEL_GUID = "ms__5d074f72-c8f0-4d53-aebc-912fb066daa0-PLSModel";
    protected static final String UUID = "5d074f72-c8f0-4d53-aebc-912fb066daa0";
    protected static final String EVENT_TABLE = "Q_PLS_Modeling_Nutanix_PLS132";
    protected static final String CONTAINER_ID = "1416355548818_20011";

    protected static final String PLS_USRNAME = "bnguyen@lattice-engines.com";
    protected static final String PLS_PASSWORD = "tahoe";

    protected static final String DL_URL = "https://data-pls.lattice-engines.com/Dataloader_PLS";
    protected static final CRMTopology TOPOLOGY = CRMTopology.MARKETO;

    protected static RestTemplate magicRestTemplate = new RestTemplate();
    protected static RestTemplate restTemplate = new RestTemplate();
    protected static MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader
            = new MagicAuthenticationHeaderHttpRequestInterceptor("");
    protected static AuthorizationHeaderHttpRequestInterceptor addAuthHeader
            = new AuthorizationHeaderHttpRequestInterceptor("");

    @Autowired
    protected PlsGaManager plsGaManager;

    @Value("${upgrade.pls.url}")
    protected String plsApiHost;

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

    protected static class AuthorizationHeaderHttpRequestInterceptor implements ClientHttpRequestInterceptor {

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

    protected boolean tenantExists() {
        String url = plsApiHost + "/pls/admin/tenants";
        ArrayNode tenants = magicRestTemplate.getForObject(url, ArrayNode.class);
        for (JsonNode tenant: tenants) {
            if (TUPLE_ID.equals(tenant.get("Identifier").asText())) return true;
        }
        return false;
    }

    protected void uploadModel() {
        InputStream ins = getClass().getClassLoader().getResourceAsStream("modelsummary.json");
        assertNotNull(ins, "Testing json file is missing");

        ModelSummary data = new ModelSummary();
        Tenant fakeTenant = new Tenant();
        fakeTenant.setId("FAKE_TENANT");
        fakeTenant.setName("Fake Tenant");
        fakeTenant.setPid(-1L);
        data.setTenant(fakeTenant);
        try {
            data.setRawFile(new String(IOUtils.toByteArray(ins)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Tenant tenant = new Tenant();
        tenant.setId(TUPLE_ID);
        tenant.setName(CUSTOMER);
        loginAndAttach(PLS_USRNAME, PLS_PASSWORD, tenant);

        List response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        int beforeUpload = response.size();

        ModelSummary newSummary = restTemplate.postForObject(
                plsApiHost + "/pls/modelsummaries?raw=true", data, ModelSummary.class);
        assertNotNull(newSummary);
        response = restTemplate.getForObject(plsApiHost + "/pls/modelsummaries/", List.class);
        assertNotNull(response);
        assertEquals(response.size(), beforeUpload + 1);
    }

    private UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(plsApiHost + "/pls/login", creds,
                LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(plsApiHost + "/pls/attach", tenant, UserDocument.class);
    }
}
