package com.latticeengines.playmaker.entitymgr.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.functionalframework.PlaymakerTestNGBase;

public class PlaymakerRecommendationEntityMgrImplTestNG extends PlaymakerTestNGBase {
    private static final Logger log = LoggerFactory.getLogger(PlaymakerRecommendationEntityMgrImplTestNG.class);

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
        Assert.assertTrue(recomendations.size() > 0L);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 1, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0L);

    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationsSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 2, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSFDC() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 0, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 1, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountSfdcAndMap() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 2, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlays() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), null, 1000, 0,
                100, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), null, 1000,
                null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensions() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                0L, 1, 100, null, null, 0L, null, false);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accExt = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accExt.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(accExt.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithDetailedPaging() throws Exception {

        Map<String, Object> countResult = playMakerRecommendationEntityMgr
                .getAccountextExsionCount(tenant.getTenantName(), null, 0L, null, null, 0L);

        Long originalTotalAccExtCount = (Long) countResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(originalTotalAccExtCount > 0L);
        // will loop over maximum 250 entries
        Long totalAccExtCount = originalTotalAccExtCount > 250 ? 250 : originalTotalAccExtCount;

        Set<Integer> ids = new HashSet<>();
        List<Object> idsList = new ArrayList<>();
        List<Pair<Object, Object>> idsTimestampTupleInOrder = new ArrayList<>();
        int offset = 0;
        int max = 10;
        long startTime = 0L;
        int totalLoopsNeeded = (int) Math.ceil(totalAccExtCount / max);
        Long lastUpdatedTimeForFirstIteration = 0L;

        for (int idx = 0; idx < totalLoopsNeeded; idx++) {
            Long lastUpdatedTime = loopForAccExt(totalAccExtCount, ids, idsList, idsTimestampTupleInOrder, offset, max,
                    startTime);
            offset += max;
            if (idx == 0) {
                lastUpdatedTimeForFirstIteration = lastUpdatedTime;
            }
            log.info("idx=" + idx + ", idsList=" + idsList);
        }

        lastUpdatedTimeForFirstIteration++;
        Map<String, Object> countResult2 = playMakerRecommendationEntityMgr.getAccountextExsionCount(
                tenant.getTenantName(), null, lastUpdatedTimeForFirstIteration, null, null, 0L);

        Long originalTotalAccExtCount2 = (Long) countResult2.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);

        if (originalTotalAccExtCount2 == 0) {
            lastUpdatedTimeForFirstIteration--;
            countResult2 = playMakerRecommendationEntityMgr.getAccountextExsionCount(tenant.getTenantName(), null,
                    lastUpdatedTimeForFirstIteration, null, null, 0L);

            originalTotalAccExtCount2 = (Long) countResult2.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);

        }
        Assert.assertTrue(originalTotalAccExtCount2 > 0L);
        log.info(String.format(
                "originalTotalAccExtCount2 = %d, originalTotalAccExtCount = %d, lastUpdatedTimeForFirstIteration = %d",
                originalTotalAccExtCount2, originalTotalAccExtCount, lastUpdatedTimeForFirstIteration));
        Assert.assertTrue(originalTotalAccExtCount2 < originalTotalAccExtCount);
        // will loop over maximum 250 entries
        Long totalAccExtCount2 = originalTotalAccExtCount2 > 250 ? 250 : originalTotalAccExtCount2;

        ids = new HashSet<>();
        idsList = new ArrayList<>();
        idsTimestampTupleInOrder = new ArrayList<>();

        offset = 0;
        max = 10;
        startTime = lastUpdatedTimeForFirstIteration;
        totalLoopsNeeded = (int) Math.ceil(totalAccExtCount2 / max);
        lastUpdatedTimeForFirstIteration = 0L;

        for (int idx = 0; idx < totalLoopsNeeded; idx++) {
            Long lastUpdatedTime = loopForAccExt(totalAccExtCount2, ids, idsList, idsTimestampTupleInOrder, offset, max,
                    startTime);
            offset += max;
            if (idx == 0) {
                lastUpdatedTimeForFirstIteration = lastUpdatedTime;
            }
        }
    }

    private Long loopForAccExt(Long totalAccExtCount, Set<Integer> ids, List<Object> idsList,
            List<Pair<Object, Object>> idsTimestampTupleInOrder, int offset, int max, long startTime) {
        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                startTime, offset, max, null, null, 0L, null, false);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accExt = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);

        Assert.assertTrue(accExt.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(accExt.size() > 0L);
        Long lastUpdatedTime = Long.parseLong(result.get(PlaymakerRecommendationEntityMgr.END_KEY).toString());
        Assert.assertNotNull(lastUpdatedTime);

        if (totalAccExtCount >= offset + max) {
            Assert.assertEquals(accExt.size(), max);
        } else {
            Assert.assertEquals(accExt.size(), offset + max - totalAccExtCount);
        }

        accExt.stream().forEach(a -> {
            Integer id = (Integer) a.get(PlaymakerRecommendationEntityMgr.ID_KEY);
            Object timestamp = a.get(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY);

            Assert.assertNotNull(id);
            if (ids.contains(id)) {
                log.info(String.format(
                        "About to fail as we found repeasing id in set. RECORD[%s, %s] "
                                + "\nset = %s, \norderedIdsList = %s, \nidsTimestampTupleInOrder = %s",
                        id, timestamp, ids.toString(), idsList.toString(),
                        idsTimestampTupleInOrder.stream().map(t -> String.format("[%s,%s] ", t.getLeft(), t.getRight()))
                                .reduce((t, u) -> t + "," + u).get()));
            }
            Assert.assertFalse(ids.contains(id));
            ids.add(id);
            idsList.add(id);
            idsTimestampTupleInOrder.add(new ImmutablePair<Object, Object>(id, timestamp));
            Assert.assertTrue(ids.contains(id));
        });

        return lastUpdatedTime;
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountextExsionCount(tenant.getTenantName(),
                null, 1000L, null, null, 0L);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionSchema() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr
                .getAccountExtensionSchema(tenant.getTenantName(), null);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionColumnCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getAccountExtensionColumnCount(tenant.getTenantName(), null);

        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);

    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithContacts() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                1000L, 1, 100, null, null, 0L, null, true);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
        Assert.assertTrue(plays.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getContacts() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContacts(tenant.getTenantName(), null, 1000, 0,
                100, null, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contacts = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(contacts.size() > 0L);
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
        Assert.assertTrue(contacts.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactCountWithAccountIds() throws Exception {

        List<Integer> accountIds = new ArrayList<>();
        accountIds.add(10);
        accountIds.add(12);
        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), null,
                1000, null, accountIds);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), null,
                1000, null, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
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
        Assert.assertTrue(plays.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensionCount(tenant.getTenantName(),
                null, 1000, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionSchema() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr
                .getContactExtensionSchema(tenant.getTenantName(), null);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getContactExtensionColumnCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getContactExtensionColumnCount(tenant.getTenantName(), null);

        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);

    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValues() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValues(tenant.getTenantName(), null, 1000,
                1, 100, null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayValueCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValueCount(tenant.getTenantName(), null,
                1000, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getWorkflowTypes() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getWorkflowTypes(tenant.getTenantName(),
                null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayGroupCount() throws Exception {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayGroupCount(tenant.getTenantName(), null,
                0L);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayGroups() throws Exception {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getPlayGroups(tenant.getTenantName(), null,
                0, 0, 100);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getPlayCountWithGroupId() throws Exception {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), null, 0,
                playgroupIds);
        Assert.assertTrue(((Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
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
        Assert.assertTrue(plays.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getRecommendationCountWithPlayId() throws Exception {

        List<String> playIds = new ArrayList<>();
        playIds.add("24");
        playIds.add("43");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 0, 1, playIds);
        Assert.assertTrue(((Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
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
        Assert.assertTrue(recomendations.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCountWithAccountId() throws Exception {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr
                .getAccountextExsionCount(tenant.getTenantName(), null, 0L, accountIds, null, 0L);
        Assert.assertTrue(((Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);

    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithAccountId() throws Exception {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0L, 0, 100, accountIds, null, 0L, null, false);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithFilterBy() throws Exception {
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0L, 0, 100, null, "RECOMMENDATIONS", 0L, null, false);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null, 0L, 0, 100,
                null, "NORECOMMENDATIONS", 0L, null, false);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions2 = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions2.size() > 0L);
    }

    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionCountWithFilterBy() throws Exception {

        Map<String, Object> mapResult = playMakerRecommendationEntityMgr
                .getAccountextExsionCount(tenant.getTenantName(), null, 0L, null, "RECOMMENDATIONS", 0L);
        Long count = (Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(count > 0L);

        mapResult = playMakerRecommendationEntityMgr.getAccountextExsionCount(tenant.getTenantName(), null, 0L, null,
                "NORECOMMENDATIONS", 0L);
        count = (Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(count > 0L);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional", enabled = true)
    public void getAccountExtensionsWithSelectedColumns() throws Exception {

        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0L, 0, 100, null, null, 0L, null, false);
        Assert.assertNotNull(mapResult);
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);
        Map<String, Object> extension = accountextensions.get(0);
        Assert.assertTrue(extension.containsKey("CrmRefreshDate"));
        Assert.assertTrue(extension.containsKey("RevenueGrowth"));

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null, 0L, 0, 100,
                null, null, 0L, " yyy, CrmRefreshDate, DnBSites,xxxx, ,,,,", false);
        Assert.assertNotNull(mapResult);
        accountextensions = (List<Map<String, Object>>) mapResult.get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);
        extension = accountextensions.get(0);

        Assert.assertTrue(extension.containsKey("DnBSites"));

        Assert.assertFalse(extension.containsKey("RevenueGrowth"));
        Assert.assertFalse(extension.containsKey("Item_ID"));
        Assert.assertFalse(extension.containsKey("yyy"));

    }

}
