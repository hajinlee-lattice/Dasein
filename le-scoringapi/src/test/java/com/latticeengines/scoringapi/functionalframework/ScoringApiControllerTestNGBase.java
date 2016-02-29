package com.latticeengines.scoringapi.functionalframework;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoringapi-context.xml" })
public class ScoringApiControllerTestNGBase extends ScoringApiFunctionalTestNGBase {

    private static final String CLIENT_ID_LP = "lp";
    protected static final String TENANT_ID = "DevelopTestPLSTenant2.DevelopTestPLSTenant2.Production";

    private static final Log log = LogFactory.getLog(ScoringApiControllerTestNGBase.class);

    @Value("${scoringapi.hostport}")
    protected String apiHostPort;

    @Value("${scoringapi.auth.hostport}")
    protected String authHostPort;

    @Value("${scoringapi.playmakerapi.hostport}")
    protected String playMakerApiHostPort;

    @Autowired
    protected OAuthUserEntityMgr userEntityMgr;

    protected OAuthUser oAuthUser;

    protected OAuth2RestTemplate oAuth2RestTemplate = null;

    @BeforeClass(groups = "functional")
    public void beforeClass() {
        oAuthUser = getOAuthUser(TENANT_ID);
        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthUser.getUserId(), oAuthUser.getPassword(),
                CLIENT_ID_LP);
        OAuth2AccessToken accessToken = oAuth2RestTemplate.getAccessToken();
        System.out.println(accessToken.getValue());
        log.info(accessToken.getValue());
    }

    @AfterClass(groups = "functional")
    public void afterClass() {
        userEntityMgr.delete(oAuthUser.getUserId());
    }

    protected OAuthUser getOAuthUser(String userId) {
        OAuthUser user = null;
        try {
            user = userEntityMgr.get(userId);
        } catch (Exception ex) {
            log.info("OAuth user does not exist! userId=" + userId);
        }
        if (user == null) {
            user = new OAuthUser();
            user.setUserId(userId);
            setPassword(user, userId);
            userEntityMgr.create(user);
        } else {
            setPassword(user, userId);
            user.setPasswordExpired(false);
            userEntityMgr.update(user);
        }

        return user;
    }

    private void setPassword(OAuthUser user, String userId) {
        user.setPassword(OAuth2Utils.generatePassword());
        user.setPasswordExpiration(userEntityMgr.getPasswordExpiration(userId));
    }

}
