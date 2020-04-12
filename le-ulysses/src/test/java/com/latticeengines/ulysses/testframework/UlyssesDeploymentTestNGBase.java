package com.latticeengines.ulysses.testframework;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.Resource;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;

public abstract class UlyssesDeploymentTestNGBase extends UlyssesTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(UlyssesDeploymentTestNGBase.class);

    protected static String CLIENT_ID = UlyssesSupportedClients.CLIENT_ID_LP;
    protected static String GW_API_KEY = "TEST_GW_KEY_1";

    protected static class UlyssesSupportedClients {
        public static final String CLIENT_ID_LP = "lp";
        public static final String CLIENT_ID_PM = "playmaker";

    }

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Value("${common.test.pls.url}")
    protected String plsApiHostPort;

    @Value("${common.test.ulysses.url}")
    protected String ulyssesHostPort;

    @Resource(name = "deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Autowired
    protected Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    private OAuth2RestTemplate oAuth2RestTemplate;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException, InterruptedException {
        String featureFlag = LatticeFeatureFlag.LATTICE_INSIGHTS.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        Tenant tenant = setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, flags);

        PlaymakerTenant oAuthTenant = new PlaymakerTenant();
        oAuthTenant.setTenantName(tenant.getId());
        oAuthTenant.setExternalId(CLIENT_ID);
        oAuthTenant.setGwApiKey(GW_API_KEY);
        oAuthTenant.setJdbcDriver("");
        oAuthTenant.setJdbcUrl("");
        oauth2RestApiProxy.createTenant(oAuthTenant);

        Thread.sleep(500); // wait for replication lag

        String oneTimeKey = oauth2RestApiProxy.createAPIToken(tenant.getId());

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenant.getId(), oneTimeKey, CLIENT_ID);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(getOAuth2RestTemplate());
        log.info("Access Token: " + accessToken.getValue());
    }

    protected RestTemplate getGlobalAuthRestTemplate() {
        return deploymentTestBed.getRestTemplate();
    }

    protected Tenant setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product, Map<String, Boolean> flags)
            throws IOException {
        turnOffSslChecking();
        deploymentTestBed.bootstrapForProduct(product, flags);
        deploymentTestBed.switchToSuperAdmin();
        return deploymentTestBed.getMainTestTenant();
    }

    protected Tenant setupTestEnvironmentWithOneTenantForProduct(LatticeProduct product) throws IOException {
        turnOffSslChecking();
        deploymentTestBed.bootstrapForProduct(product);
        deploymentTestBed.switchToSuperAdmin();
        return deploymentTestBed.getMainTestTenant();
    }

    protected static void turnOffSslChecking() {
        try {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public OAuth2RestTemplate getOAuth2RestTemplate() {
        return oAuth2RestTemplate;
    }

    public String getUlyssesRestAPIPort() {
        return ulyssesHostPort;
    }

    public String getPLSRestAPIPort() {
        return plsApiHostPort;
    }
}
