package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
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

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.AccessLevel;
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

    protected static HashMap<AccessLevel, UserDocument> testingUserSessions;
    protected static List<Tenant> testingTenants;
    protected static Tenant mainTestingTenant;

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

    abstract protected String getRestAPIHostPort();
    
    
}
