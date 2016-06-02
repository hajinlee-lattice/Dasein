package com.latticeengines.playmaker.entitymgr.impl;

import java.util.ArrayList;
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
                0, 100, 0, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 1000,
                0, 100, 1, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 1000,
                0, 100, 2, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSFDC() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                1000, 0, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                1000, 1, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                1000, 2, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlays() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), 1000, 0, 100,
                null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), 1000, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }
    
    @Test(groups = "functional", enabled = true)
    public void getAccountExtensions() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                1000, 1, 100, null);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountextExsionCount(tenant.getTenantName(),
                1000, null);
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
    public void getAccountExtensionColumnCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensionColumnCount(tenant
                .getTenantName());

        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getContacts() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContacts(tenant.getTenantName(), 1000, 0, 100,
                null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contacts = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(contacts.size() > 0);
    }
    
    @Test(groups = "functional", enabled = true)
    public void getContactCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), 1000, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }
    
    @Test(groups = "functional", enabled = true)
    public void getContactExtensions() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensions(tenant.getTenantName(),
                1000, 1, 100, null);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensionCount(tenant.getTenantName(),
                1000, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionSchema() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getContactExtensionSchema(tenant
                .getTenantName());

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionColumnCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensionColumnCount(tenant
                .getTenantName());

        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);

    }
    
    @Test(groups = "functional", enabled = true)
    public void getPlayValues() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValues(tenant.getTenantName(), 1000, 1,
                100, null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValueCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValueCount(tenant.getTenantName(), 1000,
                null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getWorkflowTypes() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getWorkflowTypes(tenant.getTenantName());
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayGroupCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayGroupCount(tenant.getTenantName(), 0);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }
    
    @Test(groups = "functional", enabled = true)
    public void getPlayGroups() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getPlayGroups(tenant.getTenantName(), 0, 0, 100);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCountWithGroupId() throws Exception {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), 0,
                playgroupIds);
        Assert.assertTrue(((Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlaysWithGroupId() throws Exception {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), 0, 0, 100,
                playgroupIds);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountWithPlayId() throws Exception {

        List<Integer> playIds = new ArrayList<>();
        playIds.add(24);
        playIds.add(43);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                0, 1, playIds);
        Assert.assertTrue(((Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsWithPlayId() throws Exception {

        List<Integer> playIds = new ArrayList<>();
        playIds.add(24);
        playIds.add(43);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), 0,
                0, 100, 1, playIds);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCountWithAccountId() throws Exception {

        List<Integer> accountIds = new ArrayList<>();
        accountIds.add(10);
        accountIds.add(12);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountextExsionCount(
                tenant.getTenantName(), 0, accountIds);
        Assert.assertTrue(((Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithAccountId() throws Exception {

        List<Integer> accountIds = new ArrayList<>();
        accountIds.add(10);
        accountIds.add(12);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                0, 0, 100, accountIds);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0);

    }

    public PlaymakerTenant getTennat() {
        return PlaymakerTenantEntityMgrImplTestNG.getTenant();
    }

}
