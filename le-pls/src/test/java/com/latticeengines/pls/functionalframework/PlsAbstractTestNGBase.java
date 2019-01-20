package com.latticeengines.pls.functionalframework;

import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.UUID;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.io.IOUtils;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
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

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.ModelSummary;
import com.latticeengines.domain.exposed.security.Session;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.util.ApplicationIdUtils;
import com.latticeengines.pls.service.impl.ModelSummaryParser;
import com.latticeengines.security.exposed.TicketAuthenticationToken;
import com.latticeengines.testframework.exposed.service.GlobalAuthTestBed;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public abstract class PlsAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    protected GlobalAuthTestBed testBed;
    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    protected RestTemplate magicRestTemplate = HttpClientUtils.newRestTemplate();
    protected Tenant mainTestTenant;

    protected String marketoModelId;
    protected String eloquaModelId;
    protected static final String eloquaModelName = "PLSModel-Eloqua";
    protected static final String marketoModelName = "PLSModel";
    protected static final String modelIdPrefix = "ms__";
    protected static final String modelAppId = "application_1547946911827_0123";
    protected static final String modelJobId = ApplicationIdUtils.stripJobId(modelAppId);

    @Autowired
    private ModelSummaryParser modelSummaryParser;

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
            @Override
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            @Override
            public void checkClientTrusted(X509Certificate[] certs, String authType) {
            }

            @Override
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

    protected ModelSummary getDetails(Tenant tenant, String suffix) throws IOException {
        String file = String.format(
                "com/latticeengines/pls/functionalframework/modelsummary-%s-token.json", suffix);
        InputStream modelSummaryFileAsStream = ClassLoader.getSystemResourceAsStream(file);
        String contents = new String(IOUtils.toByteArray(modelSummaryFileAsStream));
        String uuid = UUID.randomUUID().toString();
        contents = contents.replace("{uuid}", uuid);
        contents = contents.replace("{tenantId}", tenant.getId());
        if ("eloqua".equals(suffix)) {
            eloquaModelId = modelIdPrefix + uuid + "-" + eloquaModelName;
            contents = contents.replace("{modelName}", eloquaModelName);
        } else {
            marketoModelId = modelIdPrefix + uuid + "-" + marketoModelName;
            contents = contents.replace("{modelName}", marketoModelName);
        }
        String fakePath = String.format("/user/s-analytics/customers/%s", tenant.getId());
        ModelSummary summary = modelSummaryParser.parse(fakePath, contents);
        summary.setTenant(tenant);
        return summary;
    }

    public void setMainTestTenant(Tenant mainTestTenant) {
        this.mainTestTenant = mainTestTenant;
    }

}
