package com.latticeengines.ulysses.controller;

import java.io.IOException;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class GenericResourceDeploymentTestNG extends UlyssesDeploymentTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(GenericResourceDeploymentTestNG.class);
    private final String TEST_TENANT_ID = TestFrameworkUtils.generateTenantName();
    private final String TEST_APP_ID = "test.app.id";
    OAuth2RestTemplate localOAuth2RestTemplate;

    @Override
    @BeforeClass(groups = "deployment")
    public void beforeClass() throws IOException, InterruptedException {
        PlaymakerTenant oAuthTenant = new PlaymakerTenant();
        oAuthTenant.setTenantName(TEST_TENANT_ID);
        oAuthTenant.setJdbcDriver("");
        oAuthTenant.setJdbcUrl("");
        oAuthTenant = oauth2RestApiProxy.createTenant(oAuthTenant);

        localOAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, oAuthTenant.getTenantName(),
                oAuthTenant.getTenantPassword(), UlyssesSupportedClients.CLIENT_ID_PM, TEST_APP_ID);

        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(localOAuth2RestTemplate);
        log.info("Access Token: " + accessToken.getValue());
    }

    @Test(groups = "deployment")
    public void testOauthToTenant() {
        String tenantId = localOAuth2RestTemplate
                .getForObject(getUlyssesRestAPIPort() + "/ulysses/generic/oauthtotenant", String.class);
        Assert.assertEquals(tenantId, TEST_TENANT_ID);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "deployment")
    public void testOauthToAppId() {
        Map<String, String> result = localOAuth2RestTemplate
                .getForObject(getUlyssesRestAPIPort() + "/ulysses/generic/oauthtoappid", Map.class);
        Assert.assertEquals(result.get("app_id"), TEST_APP_ID);
    }
}
