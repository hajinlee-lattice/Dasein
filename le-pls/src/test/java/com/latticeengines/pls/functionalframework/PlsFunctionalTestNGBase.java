package com.latticeengines.pls.functionalframework;

import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.SessionFactory;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.pls.Predictor;
import com.latticeengines.domain.exposed.pls.PredictorElement;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.pls.entitymanager.KeyValueEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalAuthenticationServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalSessionManagementServiceImpl;
import com.latticeengines.pls.globalauth.authentication.impl.GlobalUserManagementServiceImpl;
import com.latticeengines.pls.security.GrantedRight;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
import com.latticeengines.pls.security.TicketAuthenticationToken;
import com.latticeengines.pls.service.impl.ModelSummaryParser;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public class PlsFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(PlsFunctionalTestNGBase.class);

    @Autowired
    private GlobalAuthenticationServiceImpl globalAuthenticationService;

    @Autowired
    private GlobalSessionManagementServiceImpl globalSessionManagementService;

    @Autowired
    private GlobalUserManagementServiceImpl globalUserManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private SessionFactory sessionFactory;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Value("${pls.api.hostport}")
    private String hostPort;

    protected RestTemplate restTemplate = new RestTemplate();
    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected void createUser(String username, String email, String firstName, String lastName) {
        createUser(username, email, firstName, lastName, "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=");
    }

    protected void createUser(String username, String email, String firstName, String lastName, String password) {
        try {
            User user1 = new User();
            user1.setFirstName(firstName);
            user1.setLastName(lastName);
            user1.setEmail(email);

            Credentials user1Creds = new Credentials();
            user1Creds.setUsername(username);
            user1Creds.setPassword(password);
            assertTrue(globalUserManagementService.registerUser(user1, user1Creds));
        } catch (Exception e) {
            log.info("User " + username + " already created.");
        }
    }

    protected String getRestAPIHostPort() {
        return hostPort;
    }

    protected void grantRight(GrantedRight right, String tenant, String username) {
        try {
            globalUserManagementService.grantRight(right.getAuthority(), tenant, username);
        } catch (Exception e) {
            log.info("Right " + right + " cannot be granted.");
        }
    }

    protected void revokeRight(GrantedRight right, String tenant, String username) {
        try {
            globalUserManagementService.revokeRight(right.getAuthority(), tenant, username);
        } catch (Exception e) {
            log.info("Right " + right + " cannot be revoked.");
        }
    }

    protected void grantDefaultRights(String tenant, String username) {
        List<GrantedRight> rights = GrantedRight.getDefaultRights();
        for (GrantedRight right: rights) {
            grantRight(right, tenant, username);
        }
    }

    protected void grantAdminRights(String tenant, String username) {
        List<GrantedRight> rights = GrantedRight.getAdminRights();
        for (GrantedRight right: rights) {
            grantRight(right, tenant, username);
        }
    }

    protected static class GetHttpStatusErrorHandler implements ResponseErrorHandler {

        public GetHttpStatusErrorHandler() {
        }

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            if (response.getStatusCode() == HttpStatus.OK) {
                return false;
            }
            return true;
        }

        @Override
        public void handleError(ClientHttpResponse response) throws IOException {
            throw new RuntimeException("" + response.getStatusCode());
        }
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
            requestWrapper.getHeaders().add(RestGlobalAuthenticationFilter.AUTHORIZATION, headerValue);

            return execution.execute(requestWrapper, body);
        }

        public void setAuthValue(String headerValue) {
            this.headerValue = headerValue;
        }
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

    protected UserDocument loginAndAttach(String username) {
        return loginAndAttach(username, "admin");
    }

    protected UserDocument loginAndAttach(String username, String password) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds,
                LoginDocument.class, new Object[] {});

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));

        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", doc.getResult().getTenants().get(0),
                UserDocument.class, new Object[] {});
    }

    protected UserDocument loginAndAttach(String username, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex("admin"));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds,
            LoginDocument.class, new Object[] {});

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));

        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", tenant,
            UserDocument.class, new Object[] {});
    }

    private ModelSummary getDetails(Tenant tenant, String suffix) throws Exception {
        String file = String.format("com/latticeengines/pls/functionalframework/modelsummary-%s.json", suffix);
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(file);
        String contents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));
        String fakePath = String.format("/user/s-analytics/customers/%s-%s", tenant.getId(), suffix);
        ModelSummary summary = modelSummaryParser.parse(fakePath, contents);
        summary.setTenant(tenant);
        return summary;
    }

    protected void setupDb(String tenant1Name, String tenant2Name) throws Exception {
        setupDb(tenant1Name, tenant2Name, true);
    }

    protected void setupDb(String tenant1Name, String tenant2Name, boolean createSummaries) throws Exception {
        keyValueEntityMgr.deleteAll();
        tenantEntityMgr.deleteAll();

        if (tenant1Name != null) {
            Tenant tenant1 = new Tenant();
            tenant1.setId(tenant1Name);
            tenant1.setName(tenant1Name);
            tenantEntityMgr.create(tenant1);

            if (createSummaries) {
                ModelSummary summary1 = getDetails(tenant1, "marketo");
                modelSummaryEntityMgr.create(summary1);
            }
        }

        if (tenant2Name != null) {
            Tenant tenant2 = new Tenant();
            tenant2.setId(tenant2Name);
            tenant2.setName(tenant2Name);
            tenantEntityMgr.create(tenant2);

            if (createSummaries) {
                ModelSummary summary2 = getDetails(tenant2, "eloqua");
                Predictor s2p1 = new Predictor();
                s2p1.setApprovedUsage("Model");
                s2p1.setCategory("Construction");
                s2p1.setName("LeadSource");
                s2p1.setDisplayName("LeadSource");
                s2p1.setFundamentalType("");
                s2p1.setUncertaintyCoefficient(0.151911);
                summary2.addPredictor(s2p1);
                PredictorElement s2el1 = new PredictorElement();
                s2el1.setName("863d38df-d0f6-42af-ac0d-06e2b8a681f8");
                s2el1.setCorrelationSign(-1);
                s2el1.setCount(311L);
                s2el1.setLift(0.0);
                s2el1.setLowerInclusive(0.0);
                s2el1.setUpperExclusive(10.0);
                s2el1.setUncertaintyCoefficient(0.00313);
                s2el1.setVisible(true);
                s2p1.addPredictorElement(s2el1);

                PredictorElement s2el2 = new PredictorElement();
                s2el2.setName("7ade3995-f3da-4b83-87e6-c358ba3bdc00");
                s2el2.setCorrelationSign(1);
                s2el2.setCount(704L);
                s2el2.setLift(1.3884292375950742);
                s2el2.setLowerInclusive(10.0);
                s2el2.setUpperExclusive(1000.0);
                s2el2.setUncertaintyCoefficient(0.000499);
                s2el2.setVisible(true);
                s2p1.addPredictorElement(s2el2);

                modelSummaryEntityMgr.create(summary2);
            }
        }
    }

    protected void setupSecurityContext(ModelSummary summary) {
        setupSecurityContext(summary.getTenant());
    }

    protected void setupSecurityContext(Tenant t) {
        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        TicketAuthenticationToken token = Mockito.mock(TicketAuthenticationToken.class);
        Session session = Mockito.mock(Session.class);
        Tenant tenant = Mockito.mock(Tenant.class);
        Mockito.when(session.getTenant()).thenReturn(tenant);
        Mockito.when(tenant.getId()).thenReturn(t.getId());
        Mockito.when(tenant.getPid()).thenReturn(t.getPid());
        Mockito.when(token.getSession()).thenReturn(session);
        Mockito.when(securityContext.getAuthentication()).thenReturn(token);
        SecurityContextHolder.setContext(securityContext);

    }

}
