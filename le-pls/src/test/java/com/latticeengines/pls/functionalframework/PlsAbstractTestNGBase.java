package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.query.ExistsRestriction;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.User;
import com.latticeengines.domain.exposed.security.UserRegistration;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.Constants;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.AuthorizationHeaderHttpRequestInterceptor;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase.GetHttpStatusErrorHandler;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public abstract class PlsAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    protected static final String adminUsername = "bnguyen@lattice-engines.com";
    protected static final String adminPassword = "tahoe";
    protected static final String adminPasswordHash = "mE2oR2b7hmeO1DpsoKuxhzx/7ODE9at6um7wFqa7udg=";
    protected static final String generalUsername = "lming@lattice-engines.com";
    protected static final String generalPassword = "admin";
    protected static final String generalPasswordHash = "EETAlfvFzCdm6/t3Ro8g89vzZo6EDCbucJMTPhYgWiE=";

    protected static final Quota QUOTA = new Quota();
    protected static final String TEST_QUOTA_ID = "TEST_QUOTA_ID";
    protected static final Integer BALANCE = 100;
    protected static final Integer BALANCE_1 = 200;
    
    protected static final TargetMarket TARGET_MARKET = new TargetMarket();
    protected static final String TEST_TARGET_MARKET_NAME = "TEST_TARGET_MARKET_NAME";
    protected static final Date CREATION_DATE = new Date();
    protected static final String DESCRIPTION = "The Target Market For Functional Tests";
    protected static final Integer NUM_PROPSPECTS_DESIRED = 100;
    protected static final Integer NUM_PROPSPECTS_DESIRED_1 = 200;
    protected static final Integer NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS = 10;
    protected static final Double INTENT_SCORE_THRESHOLD = 0.5;
    protected static final Double FIT_SCORE_THRESHOLD =  0.3;
    protected static final String MODEL_ID = "MODEL_ID";
    protected static final String EVENT_COLUMN_NAME = "EVENT_COLUMN_NAME";
    protected static final Boolean DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS = false;
    protected static final Restriction ACCOUNT_FILTER = new ExistsRestriction(false, "account", new ArrayList<Restriction>());
    protected static final Restriction CONTACT_FILTER = new ExistsRestriction(false, "contact", new ArrayList<Restriction>());

    protected static HashMap<AccessLevel, UserDocument> testingUserSessions;
    protected static List<Tenant> testingTenants;
    protected static Tenant mainTestingTenant;
    protected static Tenant ALTERNATIVE_TESTING_TENANT;

    @Autowired
    private InternalTestUserService internalTestUserService;
    
    protected SecurityFunctionalTestNGBase securityTestBase = new SecurityFunctionalTestNGBase();

    @Value("${pls.test.contract}")
    protected String contractId;

    protected RestTemplate restTemplate = new RestTemplate();
    protected RestTemplate magicRestTemplate = new RestTemplate();
    
    protected AuthorizationHeaderHttpRequestInterceptor addAuthHeader = securityTestBase.getAuthHeaderInterceptor();
    protected MagicAuthenticationHeaderHttpRequestInterceptor addMagicAuthHeader = securityTestBase.getMagicAuthHeaderInterceptor();
    protected GetHttpStatusErrorHandler statusErrorHandler = securityTestBase.getStatusErrorHandler();
    
    protected List<ClientHttpRequestInterceptor> addAuthHeaders = Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader });
    protected List<ClientHttpRequestInterceptor> addMagicAuthHeaders = Arrays.asList(new ClientHttpRequestInterceptor[] { addMagicAuthHeader });

    protected boolean createAdminUserByRestCall(String tenant, String username, String email, String firstName,
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

        return magicRestTemplate.postForObject(getRestAPIHostPort() + "/pls/admin/users", userRegistrationWithTenant,
                Boolean.class);
    }

    protected UserDocument loginAndAttach(AccessLevel level, Tenant tenant) {
        String username = internalTestUserService.getUsernameForAccessLevel(level);
        return loginAndAttach(username, internalTestUserService.getGeneralPassword(), tenant);
    }

    protected UserDocument loginAndAttach(String username) {
        return loginAndAttach(username, internalTestUserService.getGeneralPassword());
    }

    protected UserDocument loginAndAttach(String username, String password) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", doc.getResult().getTenants().get(0),
                UserDocument.class);
    }

    protected UserDocument loginAndAttach(String username, Tenant tenant) {
        return loginAndAttach(username, internalTestUserService.getGeneralPassword(), tenant);
    }

    protected UserDocument loginAndAttach(String username, String password, Tenant tenant) {
        Credentials creds = new Credentials();
        creds.setUsername(username);
        creds.setPassword(DigestUtils.sha256Hex(password));

        LoginDocument doc = restTemplate.postForObject(getRestAPIHostPort() + "/pls/login", creds, LoginDocument.class);

        addAuthHeader.setAuthValue(doc.getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[]{addAuthHeader}));

        return restTemplate.postForObject(getRestAPIHostPort() + "/pls/attach", tenant, UserDocument.class);
    }

    protected void setTestingTenants() {
        if (testingTenants == null || testingTenants.isEmpty()) {
            List<String> subTenantIds = Arrays.asList(contractId + "PLSTenant1", contractId + "PLSTenant2");
            testingTenants = new ArrayList<>();
            for (String subTenantId : subTenantIds) {
                String tenantId = CustomerSpace.parse(subTenantId).toString();
                Tenant tenant = new Tenant();
                tenant.setId(tenantId);
                String name = subTenantId.endsWith("Tenant1") ? "Tenant 1" : "Tenant 2";
                tenant.setName(contractId + " " + name);
                testingTenants.add(tenant);
            }
            mainTestingTenant = testingTenants.get(0);
            ALTERNATIVE_TESTING_TENANT = testingTenants.get(1);
        }
    }

    protected void loginTestingUsersToMainTenant() {
        if (mainTestingTenant == null) {
            setTestingTenants();
        }
        if (testingUserSessions == null || testingUserSessions.isEmpty()) {
            testingUserSessions = new HashMap<>();
            for (AccessLevel level : AccessLevel.values()) {
                UserDocument uDoc = loginAndAttach(level, mainTestingTenant);
                testingUserSessions.put(level, uDoc);
            }
        }
    }

    protected void switchToSuperAdmin() {
        switchToTheSessionWithAccessLevel(AccessLevel.SUPER_ADMIN);
    }

    protected void switchToInternalAdmin() {
        switchToTheSessionWithAccessLevel(AccessLevel.INTERNAL_ADMIN);
    }

    protected void switchToInternalUser() {
        switchToTheSessionWithAccessLevel(AccessLevel.INTERNAL_USER);
    }

    protected void switchToExternalAdmin() {
        switchToTheSessionWithAccessLevel(AccessLevel.EXTERNAL_ADMIN);
    }

    protected void switchToExternalUser() { switchToTheSessionWithAccessLevel(AccessLevel.EXTERNAL_USER); }

    protected void switchToTheSessionWithAccessLevel(AccessLevel level) {
        if (testingUserSessions == null || testingUserSessions.isEmpty()) {
            loginTestingUsersToMainTenant();
        }
        UserDocument uDoc = testingUserSessions.get(level);
        if (uDoc == null) {
            throw new NullPointerException("Could not find the session with access level " + level.name());
        }
        useSessionDoc(uDoc);
    }

    protected void useSessionDoc(UserDocument doc) {
        addAuthHeader.setAuthValue(doc.getTicket().getData());
        restTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { addAuthHeader }));
        restTemplate.setErrorHandler(new GetHttpStatusErrorHandler());
    }

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

    protected static void turnOffSslChecking() throws NoSuchAlgorithmException, KeyManagementException {
        final TrustManager[] UNQUESTIONING_TRUST_MANAGER = new TrustManager[]{
                new X509TrustManager() {
                    public java.security.cert.X509Certificate[] getAcceptedIssuers(){
                        return null;
                    }
                    public void checkClientTrusted( X509Certificate[] certs, String authType ){}
                    public void checkServerTrusted( X509Certificate[] certs, String authType ){}
                }
        };
        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }

    protected abstract String getRestAPIHostPort();
    
    
}
