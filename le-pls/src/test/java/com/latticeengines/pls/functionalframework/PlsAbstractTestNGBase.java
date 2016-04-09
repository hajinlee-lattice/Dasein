package com.latticeengines.pls.functionalframework;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.List;

import javax.annotation.PreDestroy;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.testframework.security.GlobalAuthTestBed;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml" })
public abstract class PlsAbstractTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(PlsAbstractTestNGBase.class);

    protected GlobalAuthTestBed testBed;
    protected RestTemplate restTemplate = new RestTemplate();
    protected RestTemplate magicRestTemplate = new RestTemplate();
    protected Tenant mainTestTenant;

    protected void setTestBed(GlobalAuthTestBed testBed) {
        this.testBed = testBed;
        restTemplate = testBed.getRestTemplate();
        magicRestTemplate = testBed.getMagicRestTemplate();
    }

    @PreDestroy
    public void destroy() {
        log.info("In PreDestroy of " + this.getClass().getSimpleName() + " ...");
        if (testBed != null) {
            testBed.cleanup();
        }
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

}
