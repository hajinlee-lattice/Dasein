package com.latticeengines.playmaker.controller;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.impl.PlaymakerRecommendationEntityMgrImplTestNG;

@ContextConfiguration(locations = { "classpath:playmaker-context.xml", "classpath:playmaker-properties-context.xml" })
public class RecommendationResourceTestNG extends AbstractTestNGSpringContextTests {

    @Value("${playmaker.api.hostport}")
    private String hostPort;

    private RestTemplate restTemplate = null;

    private PlaymakerTenant tenant;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        restTemplate = new RestTemplate();
        tenant = PlaymakerRecommendationEntityMgrImplTestNG.getTennat();

        try {
            deleteTenantWithTenantName();
            createTenantWithTenantName();
        } catch (Exception ex) {
            System.out.println("Warning=" + ex.getMessage());
        }
    }

    @Test(groups = "deployment")
    public void getRecommendations() {
        String url = hostPort + "/playmaker/recommendations?startId=1&size=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getPlays() {
        String url = hostPort + "/playmaker/plays?startId=1&size=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getAccountExtensions() {
        String url = hostPort + "/playmaker/accountextensions?startId=1&size=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    public void createTenantWithTenantName() {
        String url = hostPort + "/playmaker/tenants";
        restTemplate.postForObject(url, tenant, Boolean.class);

        url = hostPort + "/playmaker/tenants/" + tenant.getTenantName();
        PlaymakerTenant newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNotNull(newTenant);
    }

    public void deleteTenantWithTenantName() {
        String url = hostPort + "/playmaker/tenants/" + tenant.getTenantName();
        restTemplate.delete(url);
        PlaymakerTenant newTenant = restTemplate.getForObject(url, PlaymakerTenant.class);
        Assert.assertNull(newTenant);
    }
}
