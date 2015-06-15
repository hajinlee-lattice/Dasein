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
        String url = hostPort + "/playmaker/recommendations?start=1&offset=1&maximum=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getRecommendationCount() {
        String url = hostPort + "/playmaker/recommendationcount?start=1&tenantName=" + tenant.getTenantName();
        int result = restTemplate.getForObject(url, Integer.class);
        Assert.assertTrue(result > 0);
    }

    @Test(groups = "deployment")
    public void getPlays() {
        String url = hostPort + "/playmaker/plays?start=1&offset=1&maximum=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getPlayCount() {
        String url = hostPort + "/playmaker/playcount?start=1&tenantName=" + tenant.getTenantName();
        int result = restTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(result > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensions() {
        String url = hostPort + "/playmaker/accountextensions?start=1&offset=1&maximum=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionCount() {
        String url = hostPort + "/playmaker/accountextensioncount?start=1&tenantName=" + tenant.getTenantName();
        int result = restTemplate.getForObject(url, Integer.class);
        Assert.assertNotNull(result > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionSchema() {
        String url = hostPort + "/playmaker/accountextensionschema?&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = restTemplate.getForObject(url, List.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getPlayValues() {
        String url = hostPort + "/playmaker/playvalues?start=1&offset=1&maximum=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getPlayValueCount() {
        String url = hostPort + "/playmaker/playvaluecount?start=1&tenantName=" + tenant.getTenantName();
        int result = restTemplate.getForObject(url, Integer.class);
        Assert.assertTrue(result > 0);
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
