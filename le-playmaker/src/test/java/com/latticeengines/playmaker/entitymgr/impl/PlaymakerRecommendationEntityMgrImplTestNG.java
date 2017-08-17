package com.latticeengines.playmaker.entitymgr.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.playmaker.functionalframework.PlaymakerTestNGBase;

public class PlaymakerRecommendationEntityMgrImplTestNG extends PlaymakerTestNGBase {

    @Autowired
    private PlaymakerTenantEntityMgr playMakerTenantEntityMgr;

    @Autowired
    private PlaymakerRecommendationEntityMgr playMakerRecommendationEntityMgr;

    private PlaymakerTenant tenant;

    @Override
    @BeforeClass
    @Test(groups = "functional", enabled = true)
    public void beforeClass() {
        tenant = getTenant();
        try {
            playMakerTenantEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
        }
        playMakerTenantEntityMgr.create(tenant);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsSFDC() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 0, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 1, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 2, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSFDC() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 0, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 1, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 2, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlays() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), null, 1000, 0,
                100, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), null, 1000,
                null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensions() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                1000, 1, 100, null, null, 0L, null, false);

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
                null, 1000, null, null, 0L);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionSchema() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr
                .getAccountExtensionSchema(tenant.getTenantName(), null);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionColumnCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getAccountExtensionColumnCount(tenant.getTenantName(), null);

        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithContacts() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                1000, 1, 100, null, null, 0L, null, true);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContacts() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContacts(tenant.getTenantName(), null, 1000, 0,
                100, null, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contacts = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(contacts.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactsWithAccountIds() throws Exception {
        List<Integer> accountIds = new ArrayList<>();
        accountIds.add(10);
        accountIds.add(12);
        Map<String, Object> result = playMakerRecommendationEntityMgr.getContacts(tenant.getTenantName(), null, 1000, 0,
                100, null, accountIds);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contacts = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(contacts.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactCountWithAccountIds() throws Exception {

        List<Integer> accountIds = new ArrayList<>();
        accountIds.add(10);
        accountIds.add(12);
        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), null,
                1000, null, accountIds);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), null,
                1000, null, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensions() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensions(tenant.getTenantName(), null,
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
                null, 1000, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionSchema() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr
                .getContactExtensionSchema(tenant.getTenantName(), null);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionColumnCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getContactExtensionColumnCount(tenant.getTenantName(), null);

        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValues() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValues(tenant.getTenantName(), null, 1000,
                1, 100, null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValueCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValueCount(tenant.getTenantName(), null,
                1000, null);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getWorkflowTypes() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getWorkflowTypes(tenant.getTenantName(),
                null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayGroupCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayGroupCount(tenant.getTenantName(), null,
                0);
        Assert.assertTrue(((Integer) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayGroups() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getPlayGroups(tenant.getTenantName(), null,
                0, 0, 100);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCountWithGroupId() throws Exception {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), null, 0,
                playgroupIds);
        Assert.assertTrue(((Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlaysWithGroupId() throws Exception {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), null, 0, 0,
                100, playgroupIds);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountWithPlayId() throws Exception {

        List<String> playIds = new ArrayList<>();
        playIds.add("24");
        playIds.add("43");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 0, 1, playIds);
        Assert.assertTrue(((Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsWithPlayId() throws Exception {

        List<String> playIds = new ArrayList<>();
        playIds.add("24");
        playIds.add("43");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(),
                null, 0, 0, 100, 1, playIds);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCountWithAccountId() throws Exception {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr
                .getAccountextExsionCount(tenant.getTenantName(), null, 0, accountIds, null, 0L);
        Assert.assertTrue(((Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0);

    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithAccountId() throws Exception {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0, 0, 100, accountIds, null, 0L, null, false);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithFilterBy() throws Exception {
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0, 0, 100, null, "RECOMMENDATIONS", 0L, null, false);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0);

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null, 0, 0, 100, null,
                "NORECOMMENDATIONS", 0L, null, false);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions2 = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions2.size() > 0);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCountWithFilterBy() throws Exception {

        Map<String, Object> mapResult = playMakerRecommendationEntityMgr
                .getAccountextExsionCount(tenant.getTenantName(), null, 0, null, "RECOMMENDATIONS", 0L);
        Integer count = (Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(count > 0);

        mapResult = playMakerRecommendationEntityMgr.getAccountextExsionCount(tenant.getTenantName(), null, 0, null,
                "NORECOMMENDATIONS", 0L);
        count = (Integer) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(count > 0);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithSelectedColumns() throws Exception {

        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0, 0, 100, null, null, 0L, null, false);
        Assert.assertNotNull(mapResult);
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0);
        Map<String, Object> extension = accountextensions.get(0);
        Assert.assertTrue(extension.containsKey("CrmRefreshDate"));
        Assert.assertTrue(extension.containsKey("RevenueGrowth"));

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null, 0, 0, 100, null,
                null, 0L, " yyy, CrmRefreshDate, DnBSites,xxxx, ,,,,", false);
        Assert.assertNotNull(mapResult);
        accountextensions = (List<Map<String, Object>>) mapResult.get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0);
        extension = accountextensions.get(0);

        Assert.assertTrue(extension.containsKey("DnBSites"));

        Assert.assertFalse(extension.containsKey("RevenueGrowth"));
        Assert.assertFalse(extension.containsKey("Item_ID"));
        Assert.assertFalse(extension.containsKey("yyy"));

    }

}
