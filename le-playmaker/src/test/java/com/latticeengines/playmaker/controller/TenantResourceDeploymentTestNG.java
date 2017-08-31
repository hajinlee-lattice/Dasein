package com.latticeengines.playmaker.controller;

import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.HttpClientUtils;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.functionalframework.PlaymakerTestNGBase;

public class TenantResourceDeploymentTestNG extends PlaymakerTestNGBase {

    private String tenantName = null;
    private RestTemplate restTemplate = null;
    protected OAuth2RestTemplate adminRestTemplate = null;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        super.beforeClass();
        restTemplate = HttpClientUtils.newRestTemplate();
    }

    @Test(groups = "deployment")
    public void createTenantWithTenantNameByNonAdmin() {
        String url = apiHostPort + "/playmaker/tenants";
        PlaymakerTenant newTenant = restTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);

        try {
            newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        } catch (Exception ex) {
            Assert.assertTrue(ex.getMessage().startsWith("401"));
        }
        adminRestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, newTenant.getTenantName(),
                newTenant.getTenantPassword(), "playmaker");
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void createTenantWithTenantNameByAdmin() {
        String url = apiHostPort + "/playmaker/tenants";
        PlaymakerTenant newTenant = adminRestTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
        Assert.assertNotNull(newTenant.getTenantPassword());
        System.out.println("Tenant name=" + newTenant.getTenantName() + " password=" + newTenant.getTenantPassword());
        tenantName = newTenant.getTenantName();
        adminRestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, newTenant.getTenantName(),
                newTenant.getTenantPassword(), "playmaker");
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void updateTenantWithTenantName() {
        String url = apiHostPort + "/playmaker/tenants/" + tenant.getTenantName();
        tenant.setExternalId("externalId2");
        adminRestTemplate.put(url, tenant);

        PlaymakerTenant newTenant = adminRestTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getExternalId(), "externalId2");

    }

    @Test(groups = "deployment", dependsOnMethods = "updateTenantWithTenantName")
    public void getOauthTokenToTenant() {
        String url = apiHostPort + "/playmaker/tenants/oauthtotenant";
        String tenantNameViaToken = adminRestTemplate.getForObject(url, String.class);
        Assert.assertNotNull(tenantNameViaToken);
        Assert.assertEquals(tenantNameViaToken, tenantName);
    }

    @Test(groups = "deployment", dependsOnMethods = "getOauthTokenToTenant")
    public void deleteTenantWithTenantName() {
        String url = apiHostPort + "/playmaker/tenants/" + tenant.getTenantName();
        adminRestTemplate.delete(url);
        PlaymakerTenant newTenant = adminRestTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNull(newTenant.getTenantName());
    }
}
