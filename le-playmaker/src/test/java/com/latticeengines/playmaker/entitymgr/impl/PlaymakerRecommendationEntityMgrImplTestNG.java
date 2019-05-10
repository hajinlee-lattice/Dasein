package com.latticeengines.playmaker.entitymgr.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CDLConstants;
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

    private Map<String, String> eloquaAppId;

    private int MAX_ACC_IDS = 3;

    @Override
    @BeforeClass
    @Test(groups = "functional")
    public void beforeClass() {
        eloquaAppId = new HashMap<>();
        eloquaAppId.put(CDLConstants.AUTH_APP_ID, "lattice.eloqua01234");
        tenant = getTenant();
        try {
            playMakerTenantEntityMgr.deleteByTenantName(tenant.getTenantName());
        } catch (Exception ex) {
            log.warn("Failed to delete pMaker tenant.", ex);
        }
        playMakerTenantEntityMgr.create(tenant);
    }

    @Test(groups = "functional")
    public void getRecommendationsSFDC() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 0, null, null, eloquaAppId);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0L);

    }

    @Test(groups = "functional")
    public void getRecommendationsMap() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 1, null, null, eloquaAppId);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0L);

    }

    @Test(groups = "functional")
    public void getRecommendationsSfdcAndMap() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(), null,
                1000, 0, 100, 2, null, null, eloquaAppId);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0L);
    }

    @Test(groups = "functional")
    public void getRecommendationCountSFDC() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 0, null, null, eloquaAppId);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getRecommendationCountMap() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 1, null, null, eloquaAppId);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getRecommendationCountSfdcAndMap() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 1000, 2, null, null, eloquaAppId);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getPlays() {
        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), null, 0, 0, 100,
                null, 1, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0L);
    }

    @Test(groups = "functional")
    public void getPlayCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), null, 1000,
                null, 2, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getAccountExtensions() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                0L, 1, 100, null, null, 0L, null, false, null);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accExt = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accExt.size() > 0L);
        Assert.assertTrue(accExt.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
    }

    @Test(groups = "functional")
    public void getAccountExtensionsWithDetailedPaging() {
        getAccountExtensionsWithDetailedPaging(false, null, false);
        getAccountExtensionsWithDetailedPaging(true, null, true);
        getAccountExtensionsWithDetailedPaging(false, "BAD_COLUMN", true);
        getAccountExtensionsWithDetailedPaging(false, "CrmRefreshDate", false);
        getAccountExtensionsWithDetailedPaging(false, "CrmRefreshDate,RevenueGrowth,BAD_COLUMN", false);
    }

    private void getAccountExtensionsWithDetailedPaging(boolean shouldSendEmptyColumnMapping, String columns,
                                                        boolean expectColumnsSizeEqualTo6) {
        List<String> someAccountIds = new ArrayList<>();
        Map<String, Object> countResult = playMakerRecommendationEntityMgr
                .getAccountExtensionCount(tenant.getTenantName(), null, 0L, null, null, 0L, null);

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
        int totalLoopsNeeded = (int) Math.ceil(1.0 * totalAccExtCount / max);
        Long lastUpdatedTimeForFirstIteration = 0L;

        for (int idx = 0; idx < totalLoopsNeeded; idx++) {
            Long lastUpdatedTime = loopForAccExt(totalAccExtCount, ids, idsList, idsTimestampTupleInOrder, offset, max,
                    startTime, someAccountIds, shouldSendEmptyColumnMapping, columns, expectColumnsSizeEqualTo6);
            offset += max;
            if (idx == 0) {
                lastUpdatedTimeForFirstIteration = lastUpdatedTime;
            }
            log.info(String.format("idx=%d, idsList=%s", idx, idsList));
        }

        lastUpdatedTimeForFirstIteration++;
        Map<String, Object> countResult2 = playMakerRecommendationEntityMgr.getAccountExtensionCount(
                tenant.getTenantName(), null, lastUpdatedTimeForFirstIteration, null, null, 0L, null);

        Long originalTotalAccExtCount2 = (Long) countResult2.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);

        if (originalTotalAccExtCount2 == 0) {
            lastUpdatedTimeForFirstIteration--;
            countResult2 = playMakerRecommendationEntityMgr.getAccountExtensionCount(tenant.getTenantName(), null,
                    lastUpdatedTimeForFirstIteration, null, null, 0L, null);

            originalTotalAccExtCount2 = (Long) countResult2.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);

        }
        Assert.assertTrue(originalTotalAccExtCount2 > 0L);
        log.info(String.format(
                "originalTotalAccExtCount2 = %d, originalTotalAccExtCount = %d, lastUpdatedTimeForFirstIteration = %d",
                originalTotalAccExtCount2, originalTotalAccExtCount, lastUpdatedTimeForFirstIteration));
        Assert.assertTrue(originalTotalAccExtCount2 < originalTotalAccExtCount);
        // will loop over maximum 250 entries
        long totalAccExtCount2 = originalTotalAccExtCount2 > 250 ? 250 : originalTotalAccExtCount2;

        ids = new HashSet<>();
        idsList = new ArrayList<>();
        idsTimestampTupleInOrder = new ArrayList<>();

        offset = 0;
        max = 10;
        startTime = lastUpdatedTimeForFirstIteration;
        totalLoopsNeeded = (int) Math.ceil(1.0 * totalAccExtCount2 / max);
        lastUpdatedTimeForFirstIteration = 0L;

        for (int idx = 0; idx < totalLoopsNeeded; idx++) {
            Long lastUpdatedTime = loopForAccExt(totalAccExtCount2, ids, idsList, idsTimestampTupleInOrder, offset, max,
                    startTime, someAccountIds, shouldSendEmptyColumnMapping, columns, expectColumnsSizeEqualTo6);
            offset += max;
            if (idx == 0) {
                lastUpdatedTimeForFirstIteration = lastUpdatedTime;
            }
        }
        log.info("lastUpdatedTimeForFirstIteration=" + lastUpdatedTimeForFirstIteration);
        Assert.assertEquals(MAX_ACC_IDS, someAccountIds.size());

        getAccountExtensionsWithAccIds(someAccountIds, null);
        getAccountExtensionsWithAccIds(someAccountIds, "Recommendations");
        getAccountExtensionsWithAccIds(someAccountIds, "NoRecommendations");
        getAccountExtensionsWithAccIds(null, "Recommendations");
        getAccountExtensionsWithAccIds(null, "NoRecommendations");

        Map<String, Object> countResFilterByRecommendations = playMakerRecommendationEntityMgr
                .getAccountExtensionCount(tenant.getTenantName(), null, 0L, null, "Recommendations", 0L, null);
        Long countFilterByRecommendations = (Long) countResFilterByRecommendations
                .get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Map<String, Object> countResFilterByNoRecommendations = playMakerRecommendationEntityMgr
                .getAccountExtensionCount(tenant.getTenantName(), null, 0L, null, "NoRecommendations", 0L, null);
        Long countFilterByNoRecommendations = (Long) countResFilterByNoRecommendations
                .get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(countFilterByRecommendations > 0L);
        Assert.assertTrue(countFilterByNoRecommendations > 0L);
        Assert.assertNotSame(countFilterByNoRecommendations, countFilterByRecommendations);
        Assert.assertEquals(Long.valueOf(countFilterByNoRecommendations + countFilterByRecommendations),
                originalTotalAccExtCount);

        testEmptyResultWithLargeOffset(shouldSendEmptyColumnMapping, columns, totalAccExtCount, max, startTime);
    }

    private void testEmptyResultWithLargeOffset(boolean shouldSendEmptyColumnMapping, String columns,
            Long totalAccExtCount, int max, long startTime) {
        Map<String, Object> emptyResultDueToVeryLargeOffset = playMakerRecommendationEntityMgr.getAccountExtensions(
                tenant.getTenantName(), null, startTime, (totalAccExtCount.intValue() + 100), max, null, null, 0L,
                shouldSendEmptyColumnMapping ? "" : columns, false, null);
        Assert.assertNotNull(emptyResultDueToVeryLargeOffset);
        Assert.assertEquals(emptyResultDueToVeryLargeOffset.size(), 0);
    }

    private Long loopForAccExt(Long totalAccExtCount, Set<Integer> ids, List<Object> idsList,
            List<Pair<Object, Object>> idsTimestampTupleInOrder, int offset, int max, long startTime,
            List<String> someAccountIds, boolean shouldSendEmptyColumnMapping, String columns,
            boolean expectColumnsSizeEqualTo6) {
        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                startTime, offset, max, null, null, 0L, shouldSendEmptyColumnMapping ? "" : columns, false, null);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accExt = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);

        Assert.assertTrue(accExt.size() > 0L);
        Assert.assertTrue(accExt.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));

        Long lastUpdatedTime = Long.parseLong(result.get(PlaymakerRecommendationEntityMgr.END_KEY).toString());
        Assert.assertNotNull(lastUpdatedTime);

        int accExtSize = CollectionUtils.size(accExt);
        if (totalAccExtCount >= offset + max) {
            // should be a full page
            Assert.assertEquals(accExtSize, max, String.format("accExtSize=%d, max=%d", accExtSize, max));
        } else {
            // should be a partial page
            Assert.assertEquals(accExtSize, totalAccExtCount - offset, //
                    String.format("accExtSize=%d, offset=%d, max=%d, totalAccExtCount=%d", //
                            accExtSize, offset, max, totalAccExtCount));
        }

        List<String> impFields = Arrays.asList("ID", "SfdcAccountID", "LEAccountExternalID", "LastModificationDate",
                "RowNum");
        List<Class<?>> impFieldTypes = Arrays.asList(Long.class, String.class, String.class, Long.class, Long.class);

        int rowCount = 0;
        for (Map<?, ?> a : accExt) {
            rowCount++;
            Integer id = (Integer) a.get(PlaymakerRecommendationEntityMgr.ID_KEY);
            Object timestamp = a.get(PlaymakerRecommendationEntityMgr.LAST_MODIFIATION_DATE_KEY);

            if (someAccountIds.size() < MAX_ACC_IDS) {
                someAccountIds.add(id.toString());
            }

            int idx = 0;
            for (String field : impFields) {
                Class<?> type = impFieldTypes.get(idx++);
                Assert.assertTrue(a.containsKey(field));
                Assert.assertNotNull(a.get(field));
                if (type == Long.class) {
                    Long val = Long.parseLong("" + a.get(field));
                    if (field.equals("RowNum")) {
                        Assert.assertEquals(val, Long.valueOf(rowCount + offset));
                    }
                }
            }
            Assert.assertTrue(a.containsKey("SfdcContactID"));

            if (expectColumnsSizeEqualTo6) {
                Assert.assertEquals(a.size(), 6, String.format("a.size() = %d, expected to be = %d", a.size(), 6));
            } else {
                Assert.assertTrue(a.size() > 6, String.format("a.size() = %d, expected to be > %d", a.size(), 6));
            }

            Assert.assertNotNull(id);
            if (ids.contains(id)) {
                log.info(String.format(
                        "About to fail as we found repeasing id in set. RECORD[%s, %s] "
                                + "\nset = %s, \norderedIdsList = %s, \nidsTimestampTupleInOrder = %s",
                        id, timestamp, ids.toString(), idsList.toString(),
                        idsTimestampTupleInOrder.stream() //
                                .map(t -> String.format("[%s,%s] ", t.getLeft(), t.getRight())) //
                                .reduce((t, u) -> t + "," + u).orElse(null)));
            }
            Assert.assertFalse(ids.contains(id));
            ids.add(id);
            idsList.add(id);
            idsTimestampTupleInOrder.add(new ImmutablePair<>(id, timestamp));
            Assert.assertTrue(ids.contains(id));
        }

        Assert.assertEquals(MAX_ACC_IDS, someAccountIds.size());

        return lastUpdatedTime;
    }

    private void getAccountExtensionsWithAccIds(List<String> someAccountIds, String filterBy) {
        Map<String, Object> countResult = playMakerRecommendationEntityMgr
                .getAccountExtensionCount(tenant.getTenantName(), null, 0L, someAccountIds, filterBy, 0L, null);

        Long totalAccExtCount = (Long) countResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);

        if (filterBy == null) {
            Assert.assertEquals(totalAccExtCount.intValue(), someAccountIds.size());
        } else {
            if (someAccountIds != null) {
                Assert.assertTrue(totalAccExtCount.intValue() != someAccountIds.size());
            }
            Assert.assertTrue(totalAccExtCount.intValue() > 0L);
        }

        int maximum = 250;
        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                0L, 0, maximum, someAccountIds, filterBy, 0L, null, false, null);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accExt = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);

        if (filterBy == null) {
            Assert.assertEquals(accExt.size(), someAccountIds.size());
        } else {
            if (someAccountIds != null) {
                Assert.assertTrue(accExt.size() != someAccountIds.size());
            }
            Assert.assertTrue(totalAccExtCount.intValue() > 0L);
            if (totalAccExtCount.intValue() > maximum) {
                Assert.assertEquals(accExt.size(), maximum);
            } else {
                Assert.assertEquals(accExt.size(), totalAccExtCount.intValue());
            }
        }

        Assert.assertTrue(accExt.size() > 0L);
        for (Map<String, Object> a : accExt) {
            Assert.assertTrue(a.containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
            Integer id = (Integer) a.get(PlaymakerRecommendationEntityMgr.ID_KEY);
            if (filterBy == null) {
                Assert.assertTrue(someAccountIds.contains(id.toString()));
            }
        }
    }

    @Test(groups = "functional")
    public void getAccountExtensionCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensionCount(tenant.getTenantName(),
                null, 1000L, null, null, 0L, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getAccountExtensionSchema() {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr
                .getAccountExtensionSchema(tenant.getTenantName(), null);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional")
    public void getAccountExtensionColumnCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getAccountExtensionColumnCount(tenant.getTenantName(), null);

        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);

    }

    @Test(groups = "functional")
    public void getAccountExtensionsWithContacts() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null,
                1000L, 1, 100, null, null, 0L, null, true, null);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0L);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
    }

    @Test(groups = "functional")
    public void getContacts() {
        Map<String, Object> result = playMakerRecommendationEntityMgr.getContacts(tenant.getTenantName(), null, 1000, 0,
                100, null, null, null,null, null, eloquaAppId);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contacts = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(contacts.size() > 0L);
    }

    @Test(groups = "functional")
    public void getContactsWithAccountIds() {
        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> result = playMakerRecommendationEntityMgr.getContacts(tenant.getTenantName(), null, 1000, 0,
                100, null, accountIds, null, null, null, eloquaAppId);
        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> contacts = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(contacts.size() > 0L);
    }

    @Test(groups = "functional")
    public void getContactCountWithAccountIds() {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), null,
                1000L, null, accountIds, null, null, null, eloquaAppId);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getContactCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactCount(tenant.getTenantName(), null,
                1000L, null, null, null, null, null, eloquaAppId);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getContactExtensions() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensions(tenant.getTenantName(), null,
                1000, 1, 100, null, null, null, eloquaAppId);

        Assert.assertNotNull(result);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) result
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0L);
        Assert.assertTrue(plays.get(0).containsKey(PlaymakerRecommendationEntityMgr.ID_KEY));
    }

    @Test(groups = "functional")
    public void getContactExtensionCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getContactExtensionCount(tenant.getTenantName(),
                null, 1000, null, null, null, eloquaAppId);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getContactExtensionSchema() {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr
                .getContactExtensionSchema(tenant.getTenantName(), null);

        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional")
    public void getContactExtensionColumnCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr
                .getContactExtensionColumnCount(tenant.getTenantName(), null);

        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);

    }

    @Test(groups = "functional")
    public void getPlayValues() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValues(tenant.getTenantName(), null, 1000,
                1, 100, null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional")
    public void getPlayValueCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayValueCount(tenant.getTenantName(), null,
                1000, null);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getWorkflowTypes() {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getWorkflowTypes(tenant.getTenantName(),
                null);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional")
    public void getPlayGroupCount() {

        Map<String, Object> result = playMakerRecommendationEntityMgr.getPlayGroupCount(tenant.getTenantName(), null,
                0L);
        Assert.assertTrue(((Long) result.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getPlayGroups() {

        List<Map<String, Object>> result = playMakerRecommendationEntityMgr.getPlayGroups(tenant.getTenantName(), null,
                0, 0, 100);
        Assert.assertNotNull(result);
        Assert.assertTrue(result.size() > 0L);
    }

    @Test(groups = "functional")
    public void getPlayCountWithGroupId() {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlayCount(tenant.getTenantName(), null, 0,
                playgroupIds, 1, null);
        Assert.assertTrue(((Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getPlaysWithGroupId() {

        List<Integer> playgroupIds = new ArrayList<>();
        playgroupIds.add(1);
        playgroupIds.add(2);
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getPlays(tenant.getTenantName(), null, 0, 0,
                100, playgroupIds, 1, null);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> plays = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(plays.size() > 0L);
    }

    @Test(groups = "functional")
    public void getRecommendationCountWithPlayId() {

        List<String> playIds = new ArrayList<>();
        playIds.add("24");
        playIds.add("43");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendationCount(tenant.getTenantName(),
                null, 0, 1, playIds, null, eloquaAppId);
        Assert.assertTrue(((Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);
    }

    @Test(groups = "functional")
    public void getRecommendationsWithPlayId() {

        List<String> playIds = new ArrayList<>();
        playIds.add("24");
        playIds.add("43");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getRecommendations(tenant.getTenantName(),
                null, 0, 0, 100, 1, playIds, null, eloquaAppId);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> recomendations = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(recomendations.size() > 0L);
    }

    @Test(groups = "functional")
    public void getAccountExtensionCountWithAccountId() {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr
                .getAccountExtensionCount(tenant.getTenantName(), null, 0L, accountIds, null, 0L, null);
        Assert.assertTrue(((Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY)) > 0L);

    }

    @Test(groups = "functional")
    public void getAccountExtensionsWithAccountId() {

        List<String> accountIds = new ArrayList<>();
        accountIds.add("10");
        accountIds.add("12");
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0L, 0, 100, accountIds, null, 0L, null, false, null);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);
    }

    @Test(groups = "functional")
    public void getAccountExtensionsWithFilterBy() {
        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0L, 0, 100, null, "RECOMMENDATIONS", 0L, null, false, null);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null, 0L, 0, 100,
                null, "NORECOMMENDATIONS", 0L, null, false, null);
        Assert.assertNotNull(mapResult);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> accountextensions2 = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions2.size() > 0L);
    }

    @Test(groups = "functional")
    public void getAccountExtensionCountWithFilterBy() {

        Map<String, Object> mapResult = playMakerRecommendationEntityMgr
                .getAccountExtensionCount(tenant.getTenantName(), null, 0L, null, "RECOMMENDATIONS", 0L, null);
        Long count = (Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(count > 0L);

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensionCount(tenant.getTenantName(), null, 0L, null,
                "NORECOMMENDATIONS", 0L, null);
        count = (Long) mapResult.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertTrue(count > 0L);
    }

    @SuppressWarnings("unchecked")
    @Test(groups = "functional")
    public void getAccountExtensionsWithSelectedColumns() {

        Map<String, Object> mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(),
                null, 0L, 0, 100, null, null, 0L, null, false, null);
        Assert.assertNotNull(mapResult);
        List<Map<String, Object>> accountextensions = (List<Map<String, Object>>) mapResult
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertTrue(accountextensions.size() > 0L);
        Map<String, Object> extension = accountextensions.get(0);
        Assert.assertTrue(extension.containsKey("CrmRefreshDate"));
        Assert.assertTrue(extension.containsKey("RevenueGrowth"));

        mapResult = playMakerRecommendationEntityMgr.getAccountExtensions(tenant.getTenantName(), null, 0L, 0, 100,
                null, null, 0L, " yyy, CrmRefreshDate, DnBSites,xxxx, ,,,,", false, null);
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
