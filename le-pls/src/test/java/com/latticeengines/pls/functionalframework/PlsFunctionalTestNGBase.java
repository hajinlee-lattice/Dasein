package com.latticeengines.pls.functionalframework;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.entitymanager.KeyValueEntityMgr;
import com.latticeengines.pls.entitymanager.ModelSummaryEntityMgr;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.globalauth.authentication.GlobalAuthenticationService;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;
import com.latticeengines.pls.security.AccessLevel;
import com.latticeengines.pls.security.RestGlobalAuthenticationFilter;
import com.latticeengines.pls.security.TicketAuthenticationToken;
import com.latticeengines.pls.service.UserService;
import com.latticeengines.pls.service.impl.ModelSummaryParser;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public class PlsFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(PlsFunctionalTestNGBase.class);

    protected static boolean usersInitialized = false;
    protected static final String adminUsername = "bnguyen@lattice-engines.com";
    protected static final String adminPassword = "tahoe";
    protected static final String adminPasswordHash = "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=";
    protected static final String generalUsername = "lming@lattice-engines.com";
    protected static final String generalPassword = "admin";
    protected static final String generalPasswordHash = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private ModelSummaryEntityMgr modelSummaryEntityMgr;

    @Autowired
    private KeyValueEntityMgr keyValueEntityMgr;

    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @Autowired
    private ModelSummaryParser modelSummaryParser;

    @Autowired
    private UserService userService;

    @Value("${pls.api.hostport}")
    private String hostPort;

    protected RestTemplate restTemplate = new RestTemplate();
    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");

    protected void createUser(String username, String email, String firstName, String lastName) {
        createUser(username, email, firstName, lastName, generalPasswordHash);
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

    protected boolean createTenantByRestCall(String tenantName) {
        Tenant tenant = new Tenant();
        tenant.setId(tenantName);
        tenant.setName(tenantName);
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));
        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class, new HashMap<>());
    }

    protected boolean createAdminUserByRestCall(String tenant, String username, String email, String firstName, String lastName,
            String password) {
        UserRegistrationWithTenant userRegistrationWithTenant = new UserRegistrationWithTenant();
        userRegistrationWithTenant.setTenant(tenant);
        UserRegistration userRegistration = new UserRegistration();
        userRegistrationWithTenant.setUserRegistration(userRegistration);
        User user = new User();
        user.setActive(true);
        user.setEmail(email);
        user.setFirstName(firstName);
        user.setLastName(lastName);
        user.setUsername(username);
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(password);
        userRegistration.setUser(user);
        userRegistration.setCredentials(creds);

        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));


        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users",
                userRegistrationWithTenant, Boolean.class);
    }

    protected String getRestAPIHostPort() {
        return hostPort;
    }

    protected static class GetHttpStatusErrorHandler implements ResponseErrorHandler {

        public GetHttpStatusErrorHandler() {
        }

        @Override
        public boolean hasError(ClientHttpResponse response) throws IOException {
            return response.getStatusCode() != HttpStatus.OK;
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

    protected UserDocument loginAndAttachAdmin() {
        return loginAndAttach(adminUsername, adminPassword);
    }
    protected UserDocument loginAndAttachAdmin(Tenant tenant) {
        return loginAndAttach(adminUsername, adminPassword, tenant);
    }
    protected UserDocument loginAndAttachGeneral() {
        return loginAndAttach(generalUsername, generalPassword);
    }

    protected UserDocument loginAndAttach(String username) {
        return loginAndAttach(username, generalPassword);
    }

    protected UserDocument loginAndAttach(String username, String password) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));

        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", doc.getResult().getTenants().get(0),
                UserDocument.class);
    }

    protected UserDocument loginAndAttach(String username, Tenant tenant) {
        return loginAndAttach(username, generalPassword, tenant);
    }

    protected UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds,
            LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", tenant, UserDocument.class);
    }

    protected Ticket loginCreds(String username, String password){
        return globalAuthenticationService.authenticateUser(username, DigestUtils.sha256Hex(password));
    }

    protected void logoutUserDoc(UserDocument doc) { logoutTicket(doc.getTicket()); }

    protected void logoutTicket(Ticket ticket) { globalAuthenticationService.discard(ticket); }

    protected void useSessionDoc(UserDocument doc) {
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
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

    protected void setupDbUsingAdminTenantIds(boolean useTenant1, boolean useTenant2) throws Exception {
        setupDbUsingAdminTenantIds(useTenant1, useTenant2, true);
    }

    /**
     *
     * @param useTenant1 use the first tenant of testing admin user?
     * @param useTenant2 use the second tenant of testing admin user?
     * @param createSummaries create model summaries?
     * @throws Exception
     */
    protected void setupDbUsingAdminTenantIds(boolean useTenant1, boolean useTenant2, boolean createSummaries) throws Exception {
        Ticket ticket = globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
        String tenant1Name = useTenant1 ? ticket.getTenants().get(0).getId() : null;
        String tenant2Name = useTenant2 ? ticket.getTenants().get(1).getId() : null;
        setupDb(tenant1Name, tenant2Name, createSummaries);
        globalAuthenticationService.discard(ticket);
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

    protected void makeSureUserNoExists(String username) {
        assertTrue(globalUserManagementService.deleteUser(username));
        assertNull(globalUserManagementService.getUserByUsername(username));
    }

    protected void setupUsers() {
        Ticket ticket;
        int numOfTestingTenants = 2;
        if (globalUserManagementService.getUserByEmail("bnguyen@lattice-engines.com") != null) {
            ticket = globalAuthenticationService.authenticateUser(adminUsername, DigestUtils.sha256Hex(adminPassword));
            // all tenants of bnguyen are testing tenants
            numOfTestingTenants = ticket.getTenants().size();
        } else {
            ticket = globalAuthenticationService.authenticateUser("admin", DigestUtils.sha256Hex("admin"));
        }

        // testing admin user
        User user = globalUserManagementService.getUserByEmail("bnguyen@lattice-engines.com");
        if (user == null || !user.getUsername().equals(adminUsername)) {
            globalUserManagementService.deleteUser("bnguyen");
            globalUserManagementService.deleteUser("bnguyen@lattice-engines.com");
            createUser(adminUsername, "bnguyen@lattice-engines.com", "Super", "User", adminPasswordHash);
        }

        // testing general user
        user = globalUserManagementService.getUserByEmail("lming@lattice-engines.com");
        if (user == null || !user.getUsername().equals(generalUsername)) {
            globalUserManagementService.deleteUser("lming");
            globalUserManagementService.deleteUser("lming@lattice-engines.com");
            createUser(generalUsername, "lming@lattice-engines.com", "General", "User", generalPasswordHash);
        }

        // PM admin user
        if (globalUserManagementService.getUserByEmail("tsanghavi@lattice-engines.com") == null) {
            globalUserManagementService.deleteUser("tsanghavi@lattice-engines.com");
            createUser("tsanghavi@lattice-engines.com", "tsanghavi@lattice-engines.com", "Tejas", "Sanghavi");
        }

        for (Tenant tenant: ticket.getTenants().subList(0, numOfTestingTenants)) {
            userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), adminUsername);
            userService.assignAccessLevel(AccessLevel.EXTERNAL_USER, tenant.getId(), generalUsername);
            userService.assignAccessLevel(AccessLevel.SUPER_ADMIN, tenant.getId(), "tsanghavi@lattice-engines.com");

            userService.resignAccessLevel(tenant.getId(), "admin");
        }

        usersInitialized = true;
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
