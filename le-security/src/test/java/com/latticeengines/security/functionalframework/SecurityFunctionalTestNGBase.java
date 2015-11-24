package com.latticeengines.security.functionalframework;

import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpRequest;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestExecution;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.http.client.ClientHttpResponse;
import org.springframework.http.client.support.HttpRequestWrapper;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.ResponseErrorHandler;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.Ticket;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.globalauth.GlobalAuthenticationService;
import com.latticeengines.security.exposed.globalauth.GlobalUserManagementService;
import com.latticeengines.security.exposed.service.SessionService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-security-context.xml" })
public class SecurityFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(SecurityFunctionalTestNGBase.class);

    public static final String adminUsername = "bnguyen@lattice-engines.com";
    public static final String adminPassword = "tahoe";
    public static final String adminPasswordHash = "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=";
    public static final String generalUsername = "lming@lattice-engines.com";
    public static final String generalPassword = "admin";
    public static final String generalPasswordHash = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    protected RestTemplate restTemplate = new RestTemplate();
    protected RestTemplate magicRestTemplate = new RestTemplate();

    @Value("${security.test.api.hostport}")
    private String hostPort;

    @Value("${security.test.pls.api.hostport}")
    private String plsHostPort;

    @Autowired
    private GlobalAuthenticationService globalAuthenticationService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @Autowired
    private SessionService sessionService;

    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = new AuthorizationHeaderHttpRequestInterceptor(
            "");
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = new MagicAuthenticationHeaderHttpRequestInterceptor(
            "");
    protected List<ClientHttpRequestInterceptor> addMagicAuthHeaders = Arrays.asList(new ClientHttpRequestInterceptor[]{addMagicAuthHeader});
    protected GetHttpStatusErrorHandler statusErrorHandler = new GetHttpStatusErrorHandler();

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

    public static class GetHttpStatusErrorHandler implements ResponseErrorHandler {

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

    public RestTemplate getRestTemplate() {
        return restTemplate;
    }

    public RestTemplate getMagicRestTemplate() {
        return magicRestTemplate;
    }

    public List<ClientHttpRequestInterceptor> getAddMagicAuthHeaders() {
        return addMagicAuthHeaders;
    }

    public AuthorizationHeaderHttpRequestInterceptor getAuthHeaderInterceptor() {
        return addAuthHeader;
    }

    public MagicAuthenticationHeaderHttpRequestInterceptor getMagicAuthHeaderInterceptor() {
        return addMagicAuthHeader;
    }

    public GetHttpStatusErrorHandler getStatusErrorHandler() {
        return statusErrorHandler;
    }

    public Tenant setupTenant(CustomerSpace customerSpace) throws Exception {
        deleteTenantByRestCall(customerSpace.toString());
        Tenant tenant = new Tenant();
        tenant.setId(customerSpace.toString());
        tenant.setName(customerSpace.toString());
        createTenantByRestCall(tenant);

        return tenant;
    }

    public void deleteUserByRestCall(String username) {
        String url = getPLSRestAPIHostPort() + "/pls/users/\"" + username + "\"";
        sendHttpDeleteForObject(restTemplate, url, ResponseDocument.class);
    }

    public void createTenantByRestCall(Tenant tenant) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{ addMagicAuthHeader }));
        magicRestTemplate.postForObject(getPLSRestAPIHostPort() + "/pls/admin/tenants", tenant, Boolean.class);
    }

    public void deleteTenantByRestCall(String tenantId) {
        addMagicAuthHeader.setAuthValue(Constants.INTERNAL_SERVICE_HEADERVALUE);
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{ addMagicAuthHeader }));
        sendHttpDeleteForObject(magicRestTemplate, getPLSRestAPIHostPort() + "/pls/admin/tenants/" + tenantId, Boolean.class);
    }

    public boolean createAdminUserByRestCall(String tenant, String username, String email, String firstName,
            String lastName, String password) {
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
        magicRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader }));

        return magicRestTemplate.postForObject(getPLSRestAPIHostPort() + "/pls/admin/users", userRegistrationWithTenant,
                Boolean.class);
    }

    protected String getRestAPIHostPort() {
        return hostPort;
    }

    protected String getPLSRestAPIHostPort() {
        return plsHostPort;
    }

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

    protected void makeSureUserDoesNotExist(String username) {
        assertTrue(globalUserManagementService.deleteUser(username));
        assertNull(globalUserManagementService.getUserByUsername(username));
    }

    protected Session login(String username) {
        return login(username, generalPassword);
    }

    protected Session login(String username, String password) {
        password = DigestUtils.sha256Hex(password);
        Ticket ticket = globalAuthenticationService.authenticateUser(username, password);
        return sessionService.attach(ticket);
    }

    protected void grantRight (String right, String tenantId, String username) {
        try {
            globalUserManagementService.grantRight(right, tenantId, username);
        } catch (LedpException e) {
            //ignore
        }
    }

    protected UserDocument loginAndAttach(String username) {
        return loginAndAttach(username, generalPassword);
    }

    protected UserDocument loginAndAttach(String username, String password) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(getRestAPIHostPort() + "/attach", doc.getResult().getTenants().get(0),
                UserDocument.class);
    }

    protected UserDocument loginAndAttach(String username, Tenant tenant) {
        return loginAndAttach(username, generalPassword, tenant);
    }

    protected UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/login", creds,
                LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(getRestAPIHostPort() + "/attach", tenant, UserDocument.class);
    }

    protected void useSessionDoc(UserDocument doc) {
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
    }

    protected void logoutUserDoc(UserDocument doc) { logoutTicket(doc.getTicket()); }

    protected void logoutTicket(Ticket ticket) { globalAuthenticationService.discard(ticket); }

    protected static <T> T sendHttpDeleteForObject(RestTemplate restTemplate, String url, Class<T> responseType) {
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.DELETE, jsonRequestEntity(""), responseType);
        return response.getBody();
    }

    protected static <T> T sendHttpPutForObject(RestTemplate restTemplate, String url, Object payload, Class<T> responseType) {
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.PUT,
                jsonRequestEntity(payload), responseType);
        return response.getBody();
    }

    protected static HttpEntity<String> jsonRequestEntity(Object payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        return new HttpEntity<>(JsonUtils.serialize(payload), headers);
    }

}
