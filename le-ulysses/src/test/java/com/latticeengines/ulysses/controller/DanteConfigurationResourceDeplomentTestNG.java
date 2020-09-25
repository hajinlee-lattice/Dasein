package com.latticeengines.ulysses.controller;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.admin.LatticeFeatureFlag;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.ulysses.testframework.UlyssesDeploymentTestNGBase;

public class DanteConfigurationResourceDeplomentTestNG extends UlyssesDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(DanteConfigurationResourceDeplomentTestNG.class);

    @Inject
    private CDLTestDataService cdlTestDataService;

    private DanteConfigurationDocument danteConfig;

    private String customerSpace;

    private Tenant testTenant;

    private String getDanteConfigurationResourceUrl() {
        return ulyssesHostPort + "/ulysses/danteconfiguration";
    }

    @Override
    @BeforeClass(groups = "deployment")
    public void beforeClass() throws InterruptedException {

        String featureFlag = LatticeFeatureFlag.LATTICE_INSIGHTS.getName();
        Map<String, Boolean> flags = new HashMap<>();
        flags.put(featureFlag, true);
        testTenant = setupTestEnvironment();

        PlaymakerTenant oAuthTenant = new PlaymakerTenant();
        oAuthTenant.setTenantName(testTenant.getId());
        oAuthTenant.setExternalId(CLIENT_ID);
        oAuthTenant.setGwApiKey(GW_API_KEY);
        oAuthTenant.setJdbcDriver("");
        oAuthTenant.setJdbcUrl("");
        oauth2RestApiProxy.createTenant(oAuthTenant);

        Thread.sleep(500); // wait for replication lag

        String oneTimeKey = oauth2RestApiProxy.createAPIToken(testTenant.getId());

        oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, testTenant.getId(), oneTimeKey, CLIENT_ID);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(getOAuth2RestTemplate());
        log.info("Access Token: " + accessToken.getValue());

        customerSpace = CustomerSpace.shortenCustomerSpace(testTenant.getId());
        cdlTestDataService.populateMetadata(customerSpace, 5);

    }

    @Test(groups = "deployment")
    public void testGetDanteConfig() {
        danteConfig = getOAuth2RestTemplate().getForObject(getDanteConfigurationResourceUrl(),
                DanteConfigurationDocument.class);
        Assert.assertNotNull(danteConfig);
    }

}
