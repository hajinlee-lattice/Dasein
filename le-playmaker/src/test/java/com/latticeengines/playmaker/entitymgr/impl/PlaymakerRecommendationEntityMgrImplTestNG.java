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

@ContextConfiguration(locations = { "classpath:playmaker-context.xml", "classpath:playmaker-properties-context.xml" })
public class PlaymakerRecommendationEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private PlaymakerTenantEntityMgr playMakerTenantEntityMgr;

    @Autowired
    private PlaymakerRecommendationEntityMgr playMakerRecommendationEntityMgr;

    private PlaymakerTenant tenant;

    @BeforeClass
    public void beforeClass() {
        tenant = getTennat();
        try {
            playMakerTenantEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
        }
        playMakerTenantEntityMgr.create(tenant);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendations() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 1000,
                0, 100);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCount() throws Exception {

        int result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(), 1000);
        Assert.assertTrue(result > 0);
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

        int result = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), 1000);

        Assert.assertTrue(result > 0);
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

        int result = playMakerRecommendationEntityMgr.getAccountextensionCount(tenant.getTenantName(), 1000);
        Assert.assertTrue(result > 0);
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

        int result = playMakerRecommendationEntityMgr.getPlayValueCount(tenant.getTenantName(), 1000);
        Assert.assertTrue(result > 0);
    }

    public static PlaymakerTenant getTennat() {
        PlaymakerTenant tenant = new PlaymakerTenant();
        tenant.setExternalId("externalId");
        tenant.setJdbcDriver("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        tenant.setJdbcPassword("playmaker");
        tenant.setJdbcUrl("jdbc:sqlserver://10.41.1.82;instanceName=SQL2012STD;databaseName=ADEDTBDd70064747nA26263627n1");
        tenant.setJdbcUserName("playmaker");
        tenant.setTenantName("playmaker");
        return tenant;
    }

}
