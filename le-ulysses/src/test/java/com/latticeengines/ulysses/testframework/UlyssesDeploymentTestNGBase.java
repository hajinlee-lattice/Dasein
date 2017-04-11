package com.latticeengines.ulysses.testframework;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.HashMap;
import java.util.Map;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.testframework.security.impl.GlobalAuthDeploymentTestBed;

public abstract class UlyssesDeploymentTestNGBase extends UlyssesTestNGBase {
    private static final Log log = LogFactory.getLog(UlyssesDeploymentTestNGBase.class);

    private static final String CLIENT_ID_LP = "lp";

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Value("${common.test.pls.url}")
    protected String plsApiHostPort;

    @Value("${common.test.ulysses.url}")
    protected String ulyssesHostPort;

    @Autowired
    @Qualifier("deploymentTestBed")
    protected GlobalAuthDeploymentTestBed deploymentTestBed;

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    private OAuth2RestTemplate oAuth2RestTemplate;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException {
        String featureFlag = LatticeFeatureFlag.LATTICE_INSIGHTS.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        Tenant tenant = setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3, flags);
        String oneTimeKey = oauth2RestApiProxy.createAPIToken(tenant.getId());

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenant.getId(), oneTimeKey, CLIENT_ID_LP);
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
