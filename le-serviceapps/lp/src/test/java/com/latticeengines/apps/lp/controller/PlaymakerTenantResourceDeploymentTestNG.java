package com.latticeengines.apps.lp.controller;

import static com.latticeengines.security.exposed.Constants.INTERNAL_SERVICE_HEADERVALUE;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.proxy.PlaymakerTenantProxy;
import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class PlaymakerTenantResourceDeploymentTestNG extends LPDeploymentTestNGBase {

    private String tenantName = null;

    @Inject
    private PlaymakerTenantProxy tenantProxy;

    @Value("${common.test.microservice.url}")
    protected String apiHostPort;

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    private String oneTimePassword;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        tenantName = TestFrameworkUtils.generateTenantName();
        tenantProxy.deleteTenant(tenantName);
    }

    @AfterClass(groups = "deployment")
    public void afterClass() {
        tenantProxy.deleteTenant(tenantName);
    }

    @Test(groups = "deployment")
    public void createTenantWithTenantNameByNonAdmin() {
        PlaymakerTenant tenant = getTenant();
        PlaymakerTenant newTenant = tenantProxy.createTenant(tenant);
        oneTimePassword = newTenant.getTenantPassword();
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getTenantName(), tenantName);
        Assert.assertNotNull(oneTimePassword);
        Assert.assertEquals(newTenant.getExternalId(), "externalId1");
        System.out.println("Tenant name=" + newTenant.getTenantName() + " password=" + newTenant.getTenantPassword());

        newTenant = tenantProxy.getTenant(tenantName);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getTenantName(), tenantName);
        Assert.assertNull(newTenant.getTenantPassword());
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void updateTenantWithTenantName() {
        PlaymakerTenant tenant = tenantProxy.getTenant(tenantName);
        tenant.setExternalId("externalId2");
        tenantProxy.updateTenant(tenantName, tenant);

        PlaymakerTenant newTenant = tenantProxy.getTenant(tenantName);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getExternalId(), "externalId2");
        Assert.assertNull(newTenant.getTenantPassword());
    }

    @Test(groups = "deployment", dependsOnMethods = "updateTenantWithTenantName")
    public void getOauthTokenToTenant() {
        OAuth2RestTemplate oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenantName, oneTimePassword,
                OauthClientType.PLAYMAKER.getValue());
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        Assert.assertNotNull(accessToken);

        MagicAuthenticationHeaderHttpRequestInterceptor interceptor = new MagicAuthenticationHeaderHttpRequestInterceptor(
                INTERNAL_SERVICE_HEADERVALUE);
        oAuth2RestTemplate.getInterceptors().add(interceptor);

        String url = apiHostPort + "/lp/playmaker/tenants/oauthtotenant";
        String tenantNameViaToken = oAuth2RestTemplate.getForObject(url, String.class);
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, tenantName);
    }

    @Test(groups = "deployment", dependsOnMethods = "getOauthTokenToTenant")
    public void deleteTenantWithTenantName() {
        tenantProxy.deleteTenant(tenantName);
        PlaymakerTenant newTenant = tenantProxy.getTenant(tenantName);
        Assert.assertNull(newTenant.getTenantName());
    }

    private PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId1");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.118;instanceName=SQL2012STD;databaseName=PlayMakerDB");
        tenant.setJdbcUserName("playmaker");
        tenant.setJdbcPassword("playmaker");
        tenant.setTenantName(tenantName);
        return tenant;
    }

}
