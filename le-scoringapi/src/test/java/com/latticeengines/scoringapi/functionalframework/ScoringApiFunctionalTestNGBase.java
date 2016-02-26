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
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

import com.latticeengines.domain.exposed.oauth.OAuthUser;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

@WebAppConfiguration
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-scoringapi-context.xml" })
public class ScoringApiFunctionalTestNGBase extends AbstractTestNGSpringContextTests {

    private static final Log log = LogFactory.getLog(ScoringApiFunctionalTestNGBase.class);

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

    protected PlaymakerTenant tenant;

    @BeforeClass(groups = "functional")
    public void beforeClass() {
        tenant = getTenant();
        RestTemplate restTemplate = new RestTemplate();

        String url = playMakerApiHostPort + "/tenants";
        PlaymakerTenant newTenant = restTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, newTenant.getTenantName(),
                newTenant.getTenantPassword());
        OAuth2AccessToken accessToken = oAuth2RestTemplate.getAccessToken();
        System.out.println(accessToken.getValue());
        log.info(accessToken.getValue());

        // oAuthUser = getOAuthUser("playmaker");
        // restTemplate = OAuth2Utils.getOauthTemplate(authHostPort,
        // oAuthUser.getUserId(), oAuthUser.getPassword());
        // OAuth2AccessToken accessToken = restTemplate.getAccessToken();
        // log.info(accessToken.getValue());
    }

    @AfterClass(groups = "functional")
    public void afterClass() {
        deleteTenant();
//        userEntityMgr.delete(oAuthUser.getUserId());
    }

    private void deleteTenant() {
        String url = playMakerApiHostPort + "/tenants/" + tenant.getTenantName();
        oAuth2RestTemplate.delete(url);
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

    public static PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcPassword("playmaker");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.118;instanceName=SQL2012STD;databaseName=PlayMakerDB");
        tenant.setJdbcUserName("playmaker");
        tenant.setTenantName("playmaker");
        return tenant;
    }

}
