package com.latticeengines.playmaker.controller;

import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.functionalframework.BasePlaymakerFunctionalTestNG;

public class TenantResourceTestNG extends BasePlaymakerFunctionalTestNG {

    private RestTemplate restTemplate = null;

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
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantNameByNonAdmin")
    public void createTenantWithTenantNameByAdmin() {
        String url = apiHostPort + "/tenants";
        PlaymakerTenant newTenant = adminRestTemplate.postForObject(url, tenant, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
        Assert.assertNotNull(newTenant.getTenantPassword());
        Assert.assertTrue(newTenant.getTenantPassword().length() > 4);
        System.out.println("Tenant name=" + newTenant.getTenantName() + " password=" + newTenant.getTenantPassword());
        url = apiHostPort + "/tenants/" + tenant.getTenantName();
        tenant = adminRestTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNotNull(tenant);
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
