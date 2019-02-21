package com.latticeengines.proxy.exposed.oauth2;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;

@ContextConfiguration(locations = { "classpath:test-proxy-context.xml" })
public class Oauth2ApiDeploymentTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private Oauth2RestApiProxy oauthProxy;

    private static final String TenantName = "TESTTENANT";
    private static final String appId = "externalId";
    private static final Logger log = LoggerFactory.getLogger(Oauth2ApiDeploymentTestNG.class);

    @Test(groups = "deployment")
    public void testDeleteOauthToken() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setTenantName(TenantName);
        tenant.setJdbcDriver("dummy");
        tenant.setJdbcUrl("jdbc:sqlserver://localhost");
        PlaymakerTenant resultTenant = oauthProxy.createTenant(tenant);
        log.info("Result Tenant: \n" + resultTenant.toString());

        OAuth2AccessToken token = oauthProxy.createOAuth2AccessToken(TenantName, appId);
        boolean isVaild = oauthProxy.isValidOauthToken(TenantName, token.getValue());
        log.info("Before delete, token status is: " + isVaild);
        oauthProxy.deleteTenant(TenantName);
        try {
            isVaild = oauthProxy.isValidOauthToken(TenantName, token.getValue());
        } catch (Exception e) {
            isVaild = false;
            log.info("This error is expected! Because token has been removed.");
            e.printStackTrace();
        }
    }
}
