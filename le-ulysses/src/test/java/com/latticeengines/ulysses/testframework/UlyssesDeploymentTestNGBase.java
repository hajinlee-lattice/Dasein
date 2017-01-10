package com.latticeengines.ulysses.testframework;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.web.client.RestTemplate;
import org.testng.annotations.BeforeClass;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.LatticeOAuth2RestTemplateFactory;

public abstract class UlyssesDeploymentTestNGBase extends UlyssesTestNGBase {
    private static final Log log = LogFactory.getLog(UlyssesDeploymentTestNGBase.class);

    private static final String CLIENT_ID_LP = "lp";

    public abstract CustomerSpace getCustomerSpace();

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    @Value("${common.test.pls.url}")
    protected String plsApiHostPort;

    @Value("${common.test.ulysses.url}")
    protected String ulyssesHostPort;

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    protected LatticeOAuth2RestTemplateFactory latticeOAuth2RestTemplateFactory;

    protected OAuth2RestTemplate oAuth2RestTemplate;

    protected OAuthUser oAuthUser;

    protected RestTemplate restTemplate = HttpClientUtils.newRestTemplate();
    private String customerSpace;

    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException {
        oAuthUser = getOAuthUser(getCustomerSpace().toString());

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(), oAuthUser.getPassword(),
                CLIENT_ID_LP);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        log.info("Access Token: " + accessToken.getValue());
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

}
