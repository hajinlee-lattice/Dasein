package com.latticeengines.playmaker.entitymgr.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;

@ContextConfiguration(locations = { "classpath:test-playmaker-context.xml" })
public class PlaymakerRecommendationEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PlaymakerTenantEntityMgr playMakerTenantEntityMgr;

    @Autowired
    private PlaymakerRecommendationEntityMgr playMakerRecommendationEntityMgr;

    private PlaymakerTenant tenant;

    @BeforeClass
    @Test(groups = "functional", enabled = true)
    public void beforeClass() {
        tenant = getTennat();
        try {
            playMakerTenantEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
        }
        playMakerTenantEntityMgr.create(tenant);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsSFDC() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 1000,
                0, 100, 0);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 1000,
                0, 100, 1);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 1000,
                0, 100, 2);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSFDC() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                1000, 0);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                1000, 1);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                1000, 2);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlays() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), 1000, 0, 100);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), 1000);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensions() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountextensions(tenant.getTenantName(),
                1000, 1, 100);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountextensionCount(tenant.getTenantName(),
                1000);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionSchema() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getAccountExtensionSchema(tenant
                .getTenantName());

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValues() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValues(tenant.getTenantName(), 1000, 1,
                100);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValueCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValueCount(tenant.getTenantName(), 1000);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getWorkflowTypes() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getWorkflowTypes(tenant.getTenantName());
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    public static PlaymakerTenant getTennat() {
        return PlaymakerTenantEntityMgrImplTestNG.getTenant();
    }

}
