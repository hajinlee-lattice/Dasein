package com.latticeengines.ulysses.testframework;

import java.io.IOException;
import java.security.cert.X509Certificate;

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

import com.latticeengines.domain.exposed.admin.LatticeProduct;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;
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
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    private OAuth2RestTemplate oAuth2RestTemplate;

    protected OAuthUser oAuthUser;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException {
        Tenant tenant = setupTestEnvironmentWithOneTenantForProduct(LatticeProduct.LPA3);
        oAuthUser = getOAuthUser(tenant.getId());

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(), oAuthUser.getPassword(),
                CLIENT_ID_LP);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(getOAuth2RestTemplate());
        log.info("Access Token: " + accessToken.getValue());
    }

    protected RestTemplate getGlobalAuthRestTemplate() {
        return deploymentTestBed.getRestTemplate();
    }

    protected OAuthUser getOAuthUser(String userId) {
        OAuthUser user = oAuthUserEntityMgr.get(userId);
        if (user == null) {
            user = new OAuthUser();
            user.setUserId(userId);
            setPassword(user, userId);
            oAuthUserEntityMgr.create(user);
        } else {
            setPassword(user, userId);
            user.setPasswordExpired(false);
            oAuthUserEntityMgr.update(user);
        }

        return user;
    }

    private void setPassword(OAuthUser user, String userId) {
        user.setPassword(OAuth2Utils.generatePassword());
        user.setPasswordExpiration(oAuthUserEntityMgr.getPasswordExpiration(userId));
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

    public String getRestAPIHostPort() {
        return ulyssesHostPort;
    }

}
