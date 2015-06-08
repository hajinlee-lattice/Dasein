package com.latticeengines.playmaker.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.impl.PlaymakerEntityMgrImplTestNG;

@ContextConfiguration(locations = { "classpath:playmaker-context.xml", "classpath:playmaker-properties-context.xml" })
public class TenantResourceTestNG extends AbstractTestNGSpringContextTests {

    @Value("${playmaker.api.hostport}")
    private String hostPort;

    private RestTemplate restTemplate = null;

    private PlaymakerTenant tenant;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        restTemplate = new RestTemplate();
        tenant = PlaymakerEntityMgrImplTestNG.getTennat();

        try {
            deleteTenantWithTenantName();
        } catch (Exception ex) {
            System.out.println("Warning=" + ex.getMessage());
        }
    }

    @Test(groups = "deployment")
    public void createTenantWithTenantName() {
        String url = hostPort + "/playmaker/tenants";
        restTemplate.postForObject(url, tenant, Boolean.class);

        url = hostPort + "/playmaker/tenants/tenantName";
        PlaymakerTenant newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
    }

    @Test(groups = "deployment", dependsOnMethods = "createTenantWithTenantName")
    public void updateTenantWithTenantName() {
        String url = hostPort + "/playmaker/tenants/tenantName";
        tenant.setExternalId("externalId2");
        restTemplate.put(url, tenant);
        PlaymakerTenant newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
        Assert.assertEquals(newTenant.getExternalId(), "externalId2");

    }

    @Test(groups = "deployment", dependsOnMethods = "updateTenantWithTenantName")
    public void deleteTenantWithTenantName() {
        String url = hostPort + "/playmaker/tenants/tenantName";
        restTemplate.delete(url);
        PlaymakerTenant newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNull(newTenant);
    }
}
