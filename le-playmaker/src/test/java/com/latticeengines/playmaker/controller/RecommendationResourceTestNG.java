package com.latticeengines.playmaker.controller;

import java.util.List;
import java.util.Map;

import org.springframework.security.oauth2.client.OAuth2RestTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.functionalframework.BasePlaymakerFunctionalTestNG;

public class RecommendationResourceTestNG extends BasePlaymakerFunctionalTestNG {

    private OAuth2RestTemplate restTemplate = null;

    @BeforeClass(groups = "deployment")
    public void beforeClass() {
        super.beforeClass();
        PlaymakerTenant newTenant = playMakerEntityMgr.create(tenant);
        restTemplate = getOauthTemplate(newTenant.getTenantName(), newTenant.getTenantPassword());
    }

    @AfterClass(groups = "deployment")
    public void afterClass() {
        playMakerEntityMgr.deleteByTenantName(tenant.getTenantName());
    }

    @Test(groups = "deployment")
    public void getRecommendations() {
        String url = apiHostPort + "/playmaker/recommendations?start=1&offset=1&maximum=100&tenantName="
                + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getRecommendationCount() {
        String url = apiHostPort + "/playmaker/recommendationcount?start=1&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getPlays() {
        String url = apiHostPort + "/playmaker/plays?start=1&offset=1&maximum=100&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getPlayCount() {
        String url = apiHostPort + "/playmaker/playcount?start=1&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensions() {
        String url = apiHostPort + "/playmaker/accountextensions?start=1&offset=1&maximum=100&tenantName="
                + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionCount() {
        String url = apiHostPort + "/playmaker/accountextensioncount?start=1&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "deployment")
    public void getAccountExtensionSchema() {
        String url = apiHostPort + "/playmaker/accountextensionschema?&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> result = restTemplate.getForObject(url, List.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getPlayValues() {
        String url = apiHostPort + "/playmaker/playvalues?start=1&offset=1&maximum=100&tenantName="
                + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment")
    public void getPlayValueCount() {
        String url = apiHostPort + "/playmaker/playvaluecount?start=1&tenantName=" + tenant.getTenantName();
        @SuppressWarnings("unchecked")
        Map<String, Object> result = restTemplate.getForObject(url, Map.class);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

}
