package com.latticeengines.apps.lp.controller;

import static com.latticeengines.security.exposed.Constants.INTERNAL_SERVICE_HEADERVALUE;

import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.security.oauth2.common.OAuth2AccessToken;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.lp.testframework.LPDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.security.exposed.MagicAuthenticationHeaderHttpRequestInterceptor;
import com.latticeengines.testframework.exposed.utils.TestFrameworkUtils;

public class PlaymakerTenantResourceDeploymentTestNG extends LPDeploymentTestNGBase {

    private String tenantName = null;

    @Inject
    private Oauth2RestApiProxy tenantProxy;

    @Value("${common.test.microservice.url}")
    protected String apiHostPort;

    @Value("${common.test.oauth.url}")
    protected String authHostPort;

    private PlaymakerTenant newTenant;

    private String appId1;
    private String orgId1;
    private String externalSysType1;
    private String appId2;
    private String orgId2;
    private String externalSysType2;
    private String appId3;
    private String orgId3;
    private String externalSysType3;

    @BeforeClass(groups = "deployment", enabled = false)
    public void beforeClass() {
        tenantName = TestFrameworkUtils.generateTenantName();
        tenantProxy.deleteTenant(tenantName);

        long currentTime = System.currentTimeMillis();

        appId1 = "a1_" + currentTime;
        orgId1 = "o1_" + currentTime;
        externalSysType1 = CDLExternalSystemType.CRM.name();

        appId2 = "a2_" + currentTime;
        orgId2 = null;
        externalSysType2 = null;

        appId3 = null;
        orgId3 = null;
        externalSysType3 = null;
    }

    @AfterClass(groups = "deployment", enabled = false)
    public void afterClass() {
        tenantProxy.deleteTenant(tenantName);
    }

    @Test(groups = "deployment", enabled = false)
    public void createTenantWithTenantNameByNonAdmin() throws InterruptedException {
        PlaymakerTenant tenant = getTenant();
        newTenant = tenantProxy.createTenant(tenant);

        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getTenantName(), tenantName);
        String oneTimePassword = newTenant.getTenantPassword();
        Assert.assertNotNull(oneTimePassword);
        Assert.assertEquals(newTenant.getExternalId(), "externalId1");
        System.out.println("Tenant name=" + newTenant.getTenantName() + " password=" + newTenant.getTenantPassword());
        Thread.sleep(500); // wait for replication lag
        newTenant = tenantProxy.getTenant(tenantName);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getTenantName(), tenantName);
        Assert.assertNull(newTenant.getTenantPassword());
    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void updateTenantWithTenantName() {
        PlaymakerTenant tenant = tenantProxy.getTenant(tenantName);
        tenant.setExternalId("externalId2");
        PlaymakerTenant newTenant = tenantProxy.updateTenant(tenantName, tenant);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getExternalId(), "externalId2");
        Assert.assertNull(newTenant.getTenantPassword());
    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = "updateTenantWithTenantName")
    public void getOauthTokenToTenant() throws InterruptedException {
        String oneTimePassword = tenantProxy.createAPIToken(newTenant.getTenantName());
        Assert.assertNotNull(oneTimePassword);
        OAuth2RestTemplate oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenantName, oneTimePassword,
                OauthClientType.PLAYMAKER.getValue(), appId1, orgId1, externalSysType1);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        Assert.assertNotNull(accessToken);
        Thread.sleep(500); // wait for replication lag

        MagicAuthenticationHeaderHttpRequestInterceptor interceptor = new MagicAuthenticationHeaderHttpRequestInterceptor(
                INTERNAL_SERVICE_HEADERVALUE);
        oAuth2RestTemplate.getInterceptors().add(interceptor);

        String url = apiHostPort + "/lp/playmaker/tenants/oauthtotenant";
        String tenantNameViaToken = oAuth2RestTemplate.getForObject(url, String.class);
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, tenantName);
    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = "getOauthTokenToTenant")
    public void getOauthTokenToAppId() throws InterruptedException {
        getOauthTokenToAppId(appId1, orgId1, externalSysType1);
        getOauthTokenToAppId(appId2, orgId2, externalSysType2);
        getOauthTokenToAppId(appId3, orgId3, externalSysType3);
    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = "getOauthTokenToAppId")
    public void getOauthTokenToOrgInfo() throws InterruptedException {
        getOauthTokenToOrgInfo(appId1, orgId1, externalSysType1);
        getOauthTokenToOrgInfo(appId2, orgId2, externalSysType2);
        getOauthTokenToOrgInfo(appId3, orgId3, externalSysType3);
    }

    @Test(groups = "deployment", enabled = false, dependsOnMethods = "getOauthTokenToOrgInfo")
    public void deleteTenantWithTenantName() throws InterruptedException {
        tenantProxy.deleteTenant(tenantName);
        Thread.sleep(500); // wait for replication lag
        PlaymakerTenant newTenant = tenantProxy.getTenant(tenantName);
        Assert.assertNull(newTenant.getTenantName());
    }

    private void getOauthTokenToOrgInfo(String appId, String orgId, String externalSysType)
            throws InterruptedException {
        String oneTimePassword = tenantProxy.createAPIToken(newTenant.getTenantName());
        Assert.assertNotNull(oneTimePassword);
        OAuth2RestTemplate oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenantName, oneTimePassword,
                OauthClientType.PLAYMAKER.getValue(), appId, orgId, externalSysType);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        Assert.assertNotNull(accessToken);
        Thread.sleep(500); // wait for replication lag

        MagicAuthenticationHeaderHttpRequestInterceptor interceptor = new MagicAuthenticationHeaderHttpRequestInterceptor(
                INTERNAL_SERVICE_HEADERVALUE);
        oAuth2RestTemplate.getInterceptors().add(interceptor);

        String url = apiHostPort + "/lp/playmaker/tenants/oauthtoorginfo";
        @SuppressWarnings("rawtypes")
        Map orgInfoViaTokenObj = oAuth2RestTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(orgInfoViaTokenObj);
        Map<String, String> orgInfoViaToken = JsonUtils.convertMap(orgInfoViaTokenObj, String.class, String.class);
        Assert.assertNotNull(orgInfoViaToken);
        if (StringUtils.isBlank(orgId)) {
            Assert.assertTrue(orgInfoViaToken.size() == 0);
        } else {
            Assert.assertTrue(orgInfoViaToken.size() > 0);
            Assert.assertEquals(orgInfoViaToken.get(CDLConstants.ORG_ID), orgId);
            Assert.assertEquals(orgInfoViaToken.get(CDLConstants.EXTERNAL_SYSTEM_TYPE), externalSysType);
        }

    }

    private void getOauthTokenToAppId(String appId, String orgId, String externalSysType) throws InterruptedException {

        String oneTimePassword = tenantProxy.createAPIToken(newTenant.getTenantName());
        Assert.assertNotNull(oneTimePassword);
        OAuth2RestTemplate oAuth2RestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, tenantName, oneTimePassword,
                OauthClientType.PLAYMAKER.getValue(), appId, orgId, externalSysType);
        OAuth2AccessToken accessToken = OAuth2Utils.getAccessToken(oAuth2RestTemplate);
        Assert.assertNotNull(accessToken);
        Thread.sleep(500); // wait for replication lag

        MagicAuthenticationHeaderHttpRequestInterceptor interceptor = new MagicAuthenticationHeaderHttpRequestInterceptor(
                INTERNAL_SERVICE_HEADERVALUE);
        oAuth2RestTemplate.getInterceptors().add(interceptor);

        String url = apiHostPort + "/lp/playmaker/tenants/oauthtoappid";
        @SuppressWarnings("rawtypes")
        Map appIdViaTokenObj = oAuth2RestTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(appIdViaTokenObj);
        Map<String, String> appIdViaToken = JsonUtils.convertMap(appIdViaTokenObj, String.class, String.class);
        Assert.assertNotNull(appIdViaToken);
        Assert.assertEquals(appIdViaToken.get(CDLConstants.AUTH_APP_ID), appId);
    }

    private PlaymakerTenant getTenant() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId1");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.118;instanceName=SQL2012STD;databaseName=PlayMakerDB");
        tenant.setJdbcUserName("playmaker");
        tenant.setJdbcPasswordEncrypt("playmaker");
        tenant.setTenantName(tenantName);
        return tenant;
    }

}
