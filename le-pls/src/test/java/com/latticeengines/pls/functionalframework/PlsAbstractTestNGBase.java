package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.Listeners;

import com.latticeengines.common.exposed.query.ExistsRestriction;
import com.latticeengines.common.exposed.query.Restriction;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.IntentScore;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryConfiguration;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOption;
import com.latticeengines.domain.exposed.pls.ProspectDiscoveryOptionName;
import com.latticeengines.domain.exposed.pls.Quota;
import com.latticeengines.domain.exposed.pls.TargetMarket;
import com.latticeengines.domain.exposed.pls.TargetMarketReportMap;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.testframework.security.GlobalAuthTestBed;
import com.latticeengines.testframework.security.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public abstract class PlsAbstractTestNGBase extends AbstractTestNGSpringContextTests {

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
    protected static final Double FIT_SCORE_THRESHOLD = 0.3;
    protected static final String MODEL_ID = "MODEL_ID";
    protected static final String EVENT_COLUMN_NAME = "EVENT_COLUMN_NAME";
    protected static final Boolean DELIVER_PROSPECTS_FROM_EXISTING_ACCOUNTS = false;
    protected static final Boolean IS_DEFAULT = false;
    protected static final Restriction ACCOUNT_FILTER = new ExistsRestriction(false, "account",
            new ArrayList<Restriction>());
    protected static final Restriction CONTACT_FILTER = new ExistsRestriction(false, "contact",
            new ArrayList<Restriction>());
    protected static final Integer OFFSET = 1;
    protected static final List<String> SELECTED_INTENT = new ArrayList<>(Arrays.asList("Intent1", "Intent2"));
    protected static final Integer MAX_PROSPECTS_PER_ACCOUNT = 3;

    protected static final List<TargetMarketReportMap> TARGET_MARKET_REPORTS = new ArrayList<>();

    protected static final ProspectDiscoveryOption PROSPECT_DISCOVERY_OPTION_1 = new ProspectDiscoveryOption();
    protected static final ProspectDiscoveryOption PROSPECT_DISCOVERY_OPTION_2 = new ProspectDiscoveryOption();
    protected static final ProspectDiscoveryOptionName OPTION_1 = ProspectDiscoveryOptionName.ProspectDeliveryObject;
    protected static final ProspectDiscoveryOptionName OPTION_2 = ProspectDiscoveryOptionName.IntentPercentage;
    protected static final String STRING_VALUE = "VALUE";
    protected static final String STRING_VALUE_1 = "VALUE_1";
    protected static final String DOUBLE_VALUE = "2.5";
    protected static final ProspectDiscoveryConfiguration PROSPECT_DISCOVERY_CONFIGURATION = new ProspectDiscoveryConfiguration(
            Arrays.asList(PROSPECT_DISCOVERY_OPTION_1, PROSPECT_DISCOVERY_OPTION_2));

    protected GlobalAuthTestBed testBed;
    protected RestTemplate restTemplate = new RestTemplate();
    protected RestTemplate magicRestTemplate = new RestTemplate();
    protected Tenant mainTestTenant;

    protected void setTestBed(GlobalAuthTestBed testBed) {
        this.testBed = testBed;
        restTemplate = testBed.getRestTemplate();
        magicRestTemplate = testBed.getMagicRestTemplate();
    }

    protected static <T> T sendHttpPutForObject(RestTemplate restTemplate, String url, Object payload,
                                                Class<T> responseType) {
        ResponseEntity<T> response = restTemplate.exchange(url, HttpMethod.PUT, jsonRequestEntity(payload),
                responseType);
        return response.getBody();
    }

    protected static HttpEntity<String> jsonRequestEntity(Object payload) {
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json");
        headers.add("Accept", "application/json");
        return new HttpEntity<>(JsonUtils.serialize(payload), headers);
    }

    protected static void turnOffSslChecking() throws NoSuchAlgorithmException, KeyManagementException {
        final TrustManager[] UNQUESTIONING_TRUST_MANAGER = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
            }
        } };
        final SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, UNQUESTIONING_TRUST_MANAGER, null);
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    }

    protected abstract String getRestAPIHostPort();

    protected List<Tenant> testTenants() {
        return testBed.getTestTenants();
    }

    protected void switchToSuperAdmin() {
        testBed.switchToSuperAdmin(mainTestTenant);
    }
    protected void switchToInternalAdmin() {
        testBed.switchToInternalAdmin(mainTestTenant);
    }
    protected void switchToInternalUser() {
        testBed.switchToInternalUser(mainTestTenant);
    }
    protected void switchToExternalAdmin() {
        testBed.switchToExternalAdmin(mainTestTenant);
    }
    protected void switchToExternalUser() {
        testBed.switchToExternalUser(mainTestTenant);
    }
    protected void switchToThirdPartyUser() {
        testBed.switchToThirdPartyUser(mainTestTenant);
    }

    protected void switchToSuperAdmin(Tenant tenant) {
        testBed.switchToSuperAdmin();
    }
    protected void switchToInternalAdmin(Tenant tenant) {
        testBed.switchToInternalAdmin();
    }
    protected void switchToInternalUser(Tenant tenant) {
        testBed.switchToInternalUser();
    }
    protected void switchToExternalAdmin(Tenant tenant) {
        testBed.switchToExternalAdmin();
    }
    protected void switchToExternalUser(Tenant tenant) {
        testBed.switchToExternalUser();
    }
    protected void switchToThirdPartyUser(Tenant tenant) {
        testBed.switchToThirdPartyUser();
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

    protected void setupSecurityContext(Tenant t, String user) {
        SecurityContext securityContext = Mockito.mock(SecurityContext.class);
        TicketAuthenticationToken token = Mockito.mock(TicketAuthenticationToken.class);
        Session session = Mockito.mock(Session.class);
        Tenant tenant = Mockito.mock(Tenant.class);
        Mockito.when(session.getTenant()).thenReturn(tenant);
        Mockito.when(session.getEmailAddress()).thenReturn(user);
        Mockito.when(tenant.getId()).thenReturn(t.getId());
        Mockito.when(tenant.getPid()).thenReturn(t.getPid());
        Mockito.when(token.getSession()).thenReturn(session);
        Mockito.when(securityContext.getAuthentication()).thenReturn(token);
        SecurityContextHolder.setContext(securityContext);
    }

}
