package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.codec.digest.DigestUtils;
import org.joda.time.DateTime;
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
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.query.ExistsRestriction;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.pls.IntentScore;
import com.latticeengines.domain.exposed.pls.LoginDocument;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketStatistics;
import com.latticeengines.domain.exposed.pls.UserDocument;
import com.latticeengines.domain.exposed.security.Credentials;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.AccessLevel;
import com.latticeengines.security.exposed.service.InternalTestUserService;
import com.latticeengines.security.functionalframework.SecurityFunctionalTestNGBase;


@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public abstract class PlsAbstractTestNGBase extends SecurityFunctionalTestNGBase {

    protected static final Quota QUOTA = new Quota();
    protected static final String TEST_QUOTA_ID = "TEST_QUOTA_ID";
    protected static final Integer BALANCE = 100;
    protected static final Integer BALANCE_1 = 200;

    protected static final TargetMarket TARGET_MARKET = new TargetMarket();
    protected static final String TEST_TARGET_MARKET_NAME = "TEST_TARGET_MARKET_NAME";
    protected static final DateTime CREATION_DATE = DateTime.now();
    protected static final SimpleDateFormat FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    protected static final String DESCRIPTION = "The Target Market For Functional Tests";
    protected static final Integer NUM_PROPSPECTS_DESIRED = 100;
    protected static final Integer NUM_PROPSPECTS_DESIRED_1 = 200;
    protected static final Integer NUM_DAYS_BETWEEN_INTENT_PROSPECT_RESENDS = 10;
    protected static final IntentScore INTENT_SCORE_THRESHOLD = IntentScore.LOW;
    protected static final Double FIT_SCORE_THRESHOLD =  0.3;
    protected static final String MODEL_ID = "MODEL_ID";
    protected static final String EVENT_COLUMN_NAME = "EVENT_COLUMN_NAME";
    protected static final Boolean DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS = false;
    protected static final Boolean IS_DEFAULT = false;
    protected static final Restriction ACCOUNT_FILTER = new ExistsRestriction(false, "account", new ArrayList<Restriction>());
    protected static final Restriction CONTACT_FILTER = new ExistsRestriction(false, "contact", new ArrayList<Restriction>());
    protected static final Integer OFFSET = 1;
    protected static final List<String> SELECTED_INTENT = new ArrayList<>(Arrays.asList("Intent1", "Intent2"));
    protected static final Integer MAX_PROSPECTS_PER_ACCOUNT = 3;

    protected static final TargetMarketStatistics TARGET_MARKET_STATISTICS = new TargetMarketStatistics();
    protected static final Double EXPECTED_LIFT = 0.2D;
    protected static final Integer NUM_COMPANIES = 45;
    protected static final Integer NUM_CUSTOMERS = 55;
    protected static final Integer NUM_ACCOUNTS = 45;
    protected static final Integer MARKET_REVENUE = 100;
    protected static final Integer REVENUE = 200;
    protected static final Boolean IS_OUT_OF_DATE = false;

    protected static final ProspectDiscoveryOption PROSPECT_DISCOVERY_OPTION_1 = new ProspectDiscoveryOption();
    protected static final ProspectDiscoveryOption PROSPECT_DISCOVERY_OPTION_2 = new ProspectDiscoveryOption();
    protected static final ProspectDiscoveryOptionName OPTION_1 = ProspectDiscoveryOptionName.ProspectDeliveryObject;
    protected static final ProspectDiscoveryOptionName OPTION_2 = ProspectDiscoveryOptionName.IntentPercentage;
    protected static final String STRING_VALUE = "VALUE";
    protected static final String STRING_VALUE_1 = "VALUE_1";
    protected static final String DOUBLE_VALUE = "2.5";
    protected static final ProspectDiscoveryConfiguration PROSPECT_DISCOVERY_CONFIGURATION =
            new ProspectDiscoveryConfiguration(Arrays.asList(PROSPECT_DISCOVERY_OPTION_1, PROSPECT_DISCOVERY_OPTION_2));

    protected static HashMap<AccessLevel, UserDocument> testingUserSessions;
    protected static List<Tenant> testingTenants;
    protected static Tenant mainTestingTenant;
    protected static Tenant ALTERNATIVE_TESTING_TENANT;

    @Value("${pls.test.contract}")
    protected String contractId;

    @Autowired
    private InternalTestUserService internalTestUserService;

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

        internalTestUserService.createAllTestUsersIfNecessaryAndReturnStandardTestersAtEachAccessLevel(testingTenants);

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
