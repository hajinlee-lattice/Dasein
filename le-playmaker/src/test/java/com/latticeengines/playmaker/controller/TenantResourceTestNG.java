package com.latticeengines.playmaker.controller;

import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;
import com.latticeengines.playmaker.functionalframework.BasePlaymakerFunctionalTestNG;

public class TenantResourceTestNG extends BasePlaymakerFunctionalTestNG {

    private RestTemplate restTemplate = null;
    protected OAuth2RestTemplate adminRestTemplate = null;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        super.beforeClass();
        restTemplate = new RestTemplate();
    }

    @Test(groups = "deployment")
    public void createTenantWithTenantNameByNonAdmin() {
        String url = apiHostPort + "/tenants";
        PlaymakerTenant newTenant = restTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);

        try {
            newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        } catch (Exception ex) {
            Assert.assertEquals(ex.getMessage(), "401 Unauthorized");
        }

        adminRestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, newTenant.getTenantName(),
                newTenant.getTenantPassword(), newTenant.getTenantName());
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void createTenantWithTenantNameByAdmin() {
        String url = apiHostPort + "/tenants";
        PlaymakerTenant newTenant = adminRestTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
        Assert.assertNotNull(newTenant.getTenantPassword());
        System.out.println("Tenant name=" + newTenant.getTenantName() + " password=" + newTenant.getTenantPassword());

        adminRestTemplate = OAuth2Utils.getOauthTemplate(authHostPort, newTenant.getTenantName(),
                newTenant.getTenantPassword(), newTenant.getTenantName());
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void updateTenantWithTenantName() {
        String url = apiHostPort + "/tenants/" + tenant.getTenantName();
        tenant.setExternalId("externalId2");
        adminRestTemplate.put(url, tenant);
        PlaymakerTenant newTenant = adminRestTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getExternalId(), "externalId2");

    }

    @Test(groups = "deployment", dependsOnMethods = "updateTenantWithTenantName")
    public void deleteTenantWithTenantName() {
        String url = apiHostPort + "/tenants/" + tenant.getTenantName();
        adminRestTemplate.delete(url);
        PlaymakerTenant newTenant = adminRestTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNull(newTenant.getTenantName());
    }
}
