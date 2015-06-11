package com.latticeengines.playmaker.entitymgr.impl;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
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

    @Test(groups = "functional", enabled = true)
    public void getRecommendations() throws Exception {

        PlaymakerTenant tenant = getTennat();
        try {
            playMakerTenantEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {

        }
        playMakerTenantEntityMgr.create(tenant);

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getRecommendations(tenant.getTenantName(), 1, 100);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

//        playMakerTenantEntityMgr.delete(tenant);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlays() throws Exception {

        PlaymakerTenant tenant = getTennat();
        try {
            playMakerTenantEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {

        }
        playMakerTenantEntityMgr.create(tenant);

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), 1, 100);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0);

//        playMakerTenantEntityMgr.delete(tenant);
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
