package com.latticeengines.playmakercore.entitymanager.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerUtils;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-playmakercore-context.xml" })
public class RecommendationEntityMgrImplTestNG extends AbstractTestNGSpringContextTests {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(RecommendationEntityMgrImplTestNG.class);

    @Inject
    private RecommendationEntityMgr recommendationEntityMgr;

    private Recommendation recommendationWithoutOrgInfo;
    private Recommendation recommendationWithOrg1;
    private Recommendation recommendationWithOrg2;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String PLAY_ID = "play__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID = "launch__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID_1 = "launch1__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID_2 = "launch2__" + CURRENT_TIME_MILLIS;
    private String ACCOUNT_ID = "account__" + CURRENT_TIME_MILLIS;
    private String SFDC_ACCOUNT_ID = "sfdcAcc__" + CURRENT_TIME_MILLIS;
    private String SFDC_ACCOUNT_ID_1 = "sfdcAcc1__" + CURRENT_TIME_MILLIS;
    private String SFDC_ACCOUNT_ID_2 = "sfdcAcc2__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_DESCRIPTION = "Recommendation done on " + CURRENT_TIME_MILLIS;
    private long TENANT_PID = CURRENT_TIME_MILLIS;
    private String CUSTOMER_SPACE = "LocalTest.LocalTest.Production";
    private String DUMMY_EMAIL = "FirstName5763@com";
    private String DUMMY_ZIP = "48098-2815";
    private String DESTINATION_SYS_TYPE_1 = "destinationSysType_1";
    private String DESTINATION_ORG_ID_1 = "destinationOrgId_1";
    private String DESTINATION_SYS_TYPE_2 = "destinationSysType_2";
    private String DESTINATION_ORG_ID_2 = "destinationOrgId_2";
    private Date LAUNCH_DATE;
    private Date LAUNCH_1_DATE;
    private Date LAUNCH_2_DATE;
    private Date T1;
    private Date T2;
    private Date T3;
    private Date T4;

    private List<Recommendation> allRecommendationsAcrossAllLaunches = null;

    private Tenant tenant;

    private int maxUpdateRows = 10;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {
        LAUNCH_DATE = PlaymakerUtils.dateFromEpochSeconds(CURRENT_TIME_MILLIS / (10 * 1000L));
        LAUNCH_1_DATE = PlaymakerUtils.dateFromEpochSeconds(CURRENT_TIME_MILLIS / (5 * 1000L));
        LAUNCH_2_DATE = PlaymakerUtils.dateFromEpochSeconds(CURRENT_TIME_MILLIS / (1 * 1000L));

        T1 = PlaymakerUtils.dateFromEpochSeconds(LAUNCH_DATE.getTime() / 1000L - 24 * 3600L);
        T2 = PlaymakerUtils.dateFromEpochSeconds(LAUNCH_DATE.getTime() / 1000L + 24 * 3600L);
        T3 = PlaymakerUtils.dateFromEpochSeconds(LAUNCH_1_DATE.getTime() / 1000L + 24 * 3600L);
        T4 = PlaymakerUtils.dateFromEpochSeconds(LAUNCH_2_DATE.getTime() / 1000L + 24 * 3600L);

        System.out.println(String.format(
                "LAUNCH_DATE = %s, LAUNCH_1_DATE = %s, LAUNCH_2_DATE = %s, " //
                        + "T1 = %s, T2 = %s, T3 = %s, T4 = %s",
                LAUNCH_DATE, LAUNCH_1_DATE, LAUNCH_2_DATE, T1, T2, T3, T4));

        recommendationWithoutOrgInfo = createRecommendationObject(LAUNCH_ID, LAUNCH_DATE, SFDC_ACCOUNT_ID, null, null,
                "A");
        recommendationWithOrg1 = createRecommendationObject(LAUNCH_ID_1, LAUNCH_1_DATE, SFDC_ACCOUNT_ID_1,
                DESTINATION_ORG_ID_1, DESTINATION_SYS_TYPE_1, "B");
        recommendationWithOrg2 = createRecommendationObject(LAUNCH_ID_2, LAUNCH_2_DATE, SFDC_ACCOUNT_ID_2,
                DESTINATION_ORG_ID_2, DESTINATION_SYS_TYPE_2, "C");

        allRecommendationsAcrossAllLaunches = Arrays.asList(recommendationWithoutOrgInfo, recommendationWithOrg1,
                recommendationWithOrg2);
        tenant = new Tenant(CUSTOMER_SPACE);
        tenant.setPid(TENANT_PID);
        MultiTenantContext.setTenant(tenant);
    }

    @AfterClass(groups = "functional")
    public void teardown() {
        allRecommendationsAcrossAllLaunches.stream().forEach(rec -> {
            try {
                if (rec.getPid() != null) {
                    recommendationEntityMgr.delete(rec);
                }
            } catch (Exception ex) {
                // ignore error in cleanup
            }
        });
    }

    @Test(groups = "functional")
    public void testCreateRecommendation() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testCreateRecommendation(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateRecommendation" })
    public void testGetRecommendationById() throws InterruptedException {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationById(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationById" })
    public void testGetRecommendationByLaunchId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationByLaunchId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByLaunchId" })
    public void testGetRecommendationByPlayId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationByPlayId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByPlayId" })
    public void testGetRecommendationAsMapByPlayId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationAsMapByPlayId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationAsMapByPlayId" })
    public void testGetRecommendationWithoutPlayId() {
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> testGetRecommendationWithoutPlayId(rec));
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationWithoutPlayId" })
    public void populateMoreRecommendations() throws InterruptedException {
        IntStream.range(0, 100).forEach(i -> testCreateRecommendation(
                createRecommendationObject(LAUNCH_ID, LAUNCH_DATE, SFDC_ACCOUNT_ID, null, null, "B")));
        IntStream.range(0, 100).forEach(i -> testCreateRecommendation(createRecommendationObject(LAUNCH_ID_1,
                LAUNCH_1_DATE, SFDC_ACCOUNT_ID_1, DESTINATION_ORG_ID_1, DESTINATION_SYS_TYPE_1, "C")));
        IntStream.range(0, 100).forEach(i -> testCreateRecommendation(createRecommendationObject(LAUNCH_ID_2,
                LAUNCH_2_DATE, SFDC_ACCOUNT_ID_2, DESTINATION_ORG_ID_2, DESTINATION_SYS_TYPE_2, "D")));
    }

    @Test(groups = "functional", dependsOnMethods = { "populateMoreRecommendations" })
    public void testGetAccountIdsByLaunchIds() {
        List<String> launchIds = new ArrayList<String>();
        allRecommendationsAcrossAllLaunches.stream()//
                .forEach(rec -> launchIds.add(rec.getLaunchId()));
        testGetAccountIdsByLaunchIds(launchIds);
    }

    @Test(groups = "functional", dependsOnMethods = { "populateMoreRecommendations" })
    public void testDeleteRecommendationWithLaunchId() {
        testDelete(false, LAUNCH_ID_1, null, 101, 101, 101, 101, 0, 101);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDeleteRecommendationWithLaunchId" })
    public void testDeleteRecommendationWithPlayId() {
        testDelete(true, PLAY_ID, T1, 101, 0, 101, 101, 0, 101);

        testDelete(true, PLAY_ID, T2, 101, 0, 101, 0, 0, 101);

        testDelete(true, PLAY_ID, T3, 0, 0, 101, 0, 0, 101);

        testDelete(true, PLAY_ID, null, 0, 0, 101, 0, 0, 0);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDeleteRecommendationWithPlayId" })
    public void testDeleteRecommendationByCutoffDate() throws InterruptedException {
        populateMoreRecommendations();
        verifyRecommendationCount(100, 100, 100);

        boolean shouldLoop = true;
        while (shouldLoop) {
            int updatedCount = recommendationEntityMgr.deleteInBulkByCutoffDate(T1, false, maxUpdateRows);
            System.out.println(String.format("maxCount = %d, updatedCount = %d", maxUpdateRows, updatedCount));
            shouldLoop = (updatedCount > 0);
        }
        verifyRecommendationCount(100, 100, 100);

        shouldLoop = true;
        while (shouldLoop) {
            int updatedCount = recommendationEntityMgr.deleteInBulkByCutoffDate(T2, false, maxUpdateRows);
            System.out.println(String.format("maxCount = %d, updatedCount = %d", maxUpdateRows, updatedCount));
            shouldLoop = (updatedCount > 0);
        }
        verifyRecommendationCount(0, 100, 100);

        shouldLoop = true;
        while (shouldLoop) {
            int updatedCount = recommendationEntityMgr.deleteInBulkByCutoffDate(T3, false, maxUpdateRows);
            System.out.println(String.format("maxCount = %d, updatedCount = %d", maxUpdateRows, updatedCount));
            shouldLoop = (updatedCount > 0);
        }
        verifyRecommendationCount(0, 0, 100);

        shouldLoop = true;
        while (shouldLoop) {
            int updatedCount = recommendationEntityMgr.deleteInBulkByCutoffDate(T4, false, maxUpdateRows);
            System.out.println(String.format("maxCount = %d, updatedCount = %d", maxUpdateRows, updatedCount));
            shouldLoop = (updatedCount > 0);
        }
        verifyRecommendationCount(0, 0, 0);
    }

    private void bulkDeleteByLaunchId(String launchId, boolean hardDelete) {
        boolean shouldLoop = true;
        while (shouldLoop) {
            int updatedCount = recommendationEntityMgr.deleteInBulkByLaunchId(launchId, hardDelete, maxUpdateRows);
            System.out.println(String.format("bulkDeleteByLaunchId - launchId = %s, " //
                    + "maxCount = %d, updatedCount = %d", launchId, maxUpdateRows, updatedCount));
            shouldLoop = (updatedCount > 0);
        }
    }

    private void bulkDeleteByPlayId(String playId, Date cutoffTimestamp, boolean hardDelete) {
        boolean shouldLoop = true;
        while (shouldLoop) {
            int updatedCount = recommendationEntityMgr.deleteInBulkByPlayId(playId, cutoffTimestamp, hardDelete,
                    maxUpdateRows);
            System.out.println(
                    String.format("bulkDeleteByPlayId - cutoffTimestamp = %s, maxCount = %d, updatedCount = %d",
                            cutoffTimestamp, maxUpdateRows, updatedCount));
            shouldLoop = (updatedCount > 0);
        }
    }

    private void testDelete(boolean isPlayId, String id, Date cutoffTimestamp, int count_b_0, int count_b_1,
            int count_b_2, int count_a_0, int count_a_1, int count_a_2) {
        verifyRecommendationCount(count_b_0, count_b_1, count_b_2);

        if (isPlayId) {
            bulkDeleteByPlayId(id, cutoffTimestamp, false);
        } else {
            bulkDeleteByLaunchId(id, false);
        }

        verifyRecommendationCount(count_a_0, count_a_1, count_a_2);
    }

    private void verifyRecommendationCount(int count_0, int count_1, int count_2) {
        List<Recommendation> recs = recommendationEntityMgr.findByLaunchId(LAUNCH_ID);
        Assert.assertNotNull(recs);
        if (count_0 == 0) {
            Assert.assertFalse(recs.size() > 0);
        } else {
            Assert.assertTrue(recs.size() > 0);
            Assert.assertEquals(recs.size(), count_0);
        }
        recs = recommendationEntityMgr.findByLaunchId(LAUNCH_ID_1);
        Assert.assertNotNull(recs);
        if (count_1 == 0) {
            Assert.assertFalse(recs.size() > 0);
        } else {
            Assert.assertTrue(recs.size() > 0);
            Assert.assertEquals(recs.size(), count_1);
        }
        recs = recommendationEntityMgr.findByLaunchId(LAUNCH_ID_2);
        Assert.assertNotNull(recs);
        if (count_2 == 0) {
            Assert.assertFalse(recs.size() > 0);
        } else {
            Assert.assertTrue(recs.size() > 0);
            Assert.assertEquals(recs.size(), count_2);
        }
    }

    private Recommendation createRecommendationObject(String launchId, Date launchDate, String sfdcAccountID,
            String destinationOrgId, String destinationSysType, String priorityDisplayName) {
        Recommendation recommendation = new Recommendation();
        recommendation.setDescription(LAUNCH_DESCRIPTION);
        recommendation.setLaunchId(launchId);
        recommendation.setPlayId(PLAY_ID);
        recommendation.setAccountId(ACCOUNT_ID);
        recommendation.setSfdcAccountID(sfdcAccountID);
        recommendation.setLeAccountExternalID(ACCOUNT_ID);
        recommendation.setPriorityDisplayName(priorityDisplayName);
        recommendation.setPriorityID(RatingBucketName.valueOf(priorityDisplayName));
        recommendation.setTenantId(TENANT_PID);
        String contacts = //
                " [ " //
                        + "  { " //
                        + "   \"Email\": \"" + DUMMY_EMAIL + "\", " //
                        + "   \"Address\": \"null Dr\", " //
                        + "   \"Phone\": \"248.813.2000\", " //
                        + "   \"State\": \"MI\", " //
                        + "   \"ZipCode\": \"" + DUMMY_ZIP + "\", " //
                        + "   \"Country\": \"USA\", " //
                        + "   \"SfdcContactID\": \"\", " //
                        + "   \"City\": \"Troy\", " //
                        + "   \"ContactID\": \"5763\", " //
                        + "   \"Name\": \"FirstName5763 LastName5763\" " //
                        + "  } " //
                        + " ] ";
        recommendation.setContacts(contacts);
        recommendation.setSynchronizationDestination(SynchronizationDestinationEnum.SFDC.toString());
        recommendation.setDestinationOrgId(destinationOrgId);
        recommendation.setDestinationSysType(destinationSysType);
        recommendation.setLaunchDate(launchDate);
        return recommendation;
    }

    private void testCreateRecommendation(Recommendation recommendation) {
        Assert.assertNotNull(recommendation.getPriorityDisplayName());
        Assert.assertNotNull(recommendation.getPriorityID());
        recommendationEntityMgr.create(recommendation);
        Assert.assertNotNull(recommendation.getRecommendationId());
        Assert.assertNotNull(recommendation.getPid());
        Assert.assertNotNull(recommendation.getPriorityDisplayName());
        Assert.assertNotNull(recommendation.getPriorityID());
        System.out.println(String.format("Created Recommendation launchId = %s, pid = %d, launchDate = %s",
                recommendation.getLaunchId(), recommendation.getPid(), recommendation.getLaunchDate()));

    }

    private void testGetRecommendationById(Recommendation originalRec) {
        MultiTenantContext.setTenant(tenant);
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // wait for replication lag
        Recommendation recommendation = recommendationEntityMgr
                .findByRecommendationId(originalRec.getRecommendationId());
        compareOriginalAndExtractedRecommendations(originalRec, recommendation);
    }

    private void testGetRecommendationByLaunchId(Recommendation originalRec) {
        MultiTenantContext.setTenant(tenant);

        List<Recommendation> recommendations = recommendationEntityMgr.findByLaunchId(originalRec.getLaunchId());
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 1);

        for (Recommendation recommendation : recommendations) {
            compareOriginalAndExtractedRecommendations(originalRec, recommendation);
        }
    }

    private void testGetAccountIdsByLaunchIds(List<String> launchIds) {
        MultiTenantContext.setTenant(tenant);
        List<Map<String,Object>> accountInfos = recommendationEntityMgr.findAccountIdsFromRecommendationByLaunchId(launchIds, 0, 0,
                1000);
        int num = recommendationEntityMgr.findAccountIdsCountFromRecommendationByLaunchId(launchIds, 0);
        System.out.print(launchIds);
        Assert.assertNotNull(accountInfos);
        System.out.println(accountInfos);
        Assert.assertTrue(num > 0);
        Assert.assertTrue(accountInfos.size() > 0);
    }

    private void testGetRecommendationByPlayId(Recommendation originalRecommendation) {
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(originalRecommendation.getPlayId());
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRecommendation.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRecommendation.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRecommendation.getDestinationSysType());
        }

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);

        Assert.assertEquals(1, recommendationCount);

        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);

        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        for (Recommendation recommendation : recommendations) {
            compareOriginalAndExtractedRecommendations(originalRecommendation, recommendation);
        }
    }

    private void testGetRecommendationAsMapByPlayId(Recommendation originalRecommendation) {
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(originalRecommendation.getPlayId());
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRecommendation.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRecommendation.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRecommendation.getDestinationSysType());
        }

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);

        Assert.assertEquals(1, recommendationCount);

        List<Map<String, Object>> recommendations = recommendationEntityMgr.findRecommendationsAsMap(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        Long lastModificationDate = 0L;

        for (Map<String, Object> recommendation : recommendations) {
            Assert.assertTrue(recommendation.size() > 0);
            Object timestamp = recommendation.get(PlaymakerConstants.LastModificationDate);
            if (!(timestamp instanceof Long)) {
                timestamp = Long.parseLong(timestamp.toString());
            }
            if ((Long) timestamp > lastModificationDate) {
                lastModificationDate = (Long) timestamp;
            }
            compareOriginalAndExtractedRecommendations(originalRecommendation, recommendation);
        }

        Date lastModificationDate2 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate + 1);
        Date lastModificationDate3 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate - 1);

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate2, //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertEquals(0, recommendationCount);

        recommendations = recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate2, //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate3, //
                SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertTrue(recommendationCount > 0);

        recommendations = recommendationEntityMgr.findRecommendationsAsMap(lastModificationDate3, //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds, orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), recommendationCount);
        compareOriginalAndExtractedRecommendations(originalRecommendation, recommendations.get(0));

    }

    private void testGetRecommendationWithoutPlayId(Recommendation originalRecommendation) {
        MultiTenantContext.setTenant(tenant);
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRecommendation.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRecommendation.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRecommendation.getDestinationSysType());
        }

        int recommendationCount = recommendationEntityMgr.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);

        Assert.assertTrue(recommendationCount > 0);
        Assert.assertEquals(recommendationCount, 1);

        int minPageSize = Math.min(10, recommendationCount);

        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0L), //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null,
                orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), minPageSize);
        Long lastModificationDate = 0L;

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
            Assert.assertNotNull(recommendation.getTenantId());
            Assert.assertEquals(recommendation.getTenantId().longValue(), TENANT_PID);
            Object timestamp = new Long(recommendation.getLastUpdatedTimestamp().getTime());
            if (!(timestamp instanceof Long)) {
                timestamp = Long.parseLong(timestamp.toString());
            }
            if ((Long) timestamp > lastModificationDate) {
                lastModificationDate = (Long) timestamp;
            }
            compareOriginalAndExtractedRecommendations(originalRecommendation, recommendation);
        }
        lastModificationDate = lastModificationDate / 1000;

        Date lastModificationDate2 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate + 1);
        @SuppressWarnings("unused")
        Date lastModificationDate3 = PlaymakerUtils.dateFromEpochSeconds(lastModificationDate - 1);

        recommendationCount = recommendationEntityMgr.findRecommendationCount(lastModificationDate2, //
                SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);

        Assert.assertEquals(0, recommendationCount);

        recommendations = recommendationEntityMgr.findRecommendations(lastModificationDate2, //
                (recommendationCount - minPageSize) < 0 ? 0 : (recommendationCount - minPageSize), //
                minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() == 0);

        // TODO - enable it once fixed
        /*
         * recommendationCount =
         * recommendationEntityMgr.findRecommendationCount(
         * lastModificationDate3, //
         * SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);
         * 
         * Assert.assertEquals(recommendationCount, 1);
         * 
         * recommendations =
         * recommendationEntityMgr.findRecommendations(lastModificationDate3, //
         * (recommendationCount - minPageSize), minPageSize,
         * SynchronizationDestinationEnum.SFDC.toString(), null, orgInfo);
         * Assert.assertNotNull(recommendations);
         * Assert.assertTrue(recommendations.size() > 0, String.format(
         * "lastModificationDate3 = %s, (recommendationCount - minPageSize) = %d, "
         * + "minPageSize = %d, orgInfo = %s, recommendations.size() = %d",
         * lastModificationDate3.toString(), (recommendationCount -
         * minPageSize), minPageSize, orgInfo, recommendations.size()));
         * compareOriginalAndExtractedRecommendations(originalRecommendation,
         * recommendations.get(0));
         */
    }

    private void compareOriginalAndExtractedRecommendations(Recommendation originalRec,
            Map<String, Object> recommendation) {
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRec.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRec.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRec.getDestinationSysType());
        }

        try {

            Assert.assertNotNull(recommendation.get(PlaymakerConstants.ID));
            Assert.assertNotNull(recommendation.get(PlaymakerConstants.Contacts));
            Assert.assertEquals(recommendation.get(PlaymakerConstants.ID), originalRec.getRecommendationId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.AccountID), originalRec.getAccountId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.Description), originalRec.getDescription());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.ID), originalRec.getId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.LaunchID), originalRec.getLaunchId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.LEAccountExternalID),
                    originalRec.getLeAccountExternalID());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.MonetaryValueIso4217ID),
                    originalRec.getMonetaryValueIso4217ID());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.PlayID), originalRec.getPlayId());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.PriorityDisplayName),
                    originalRec.getPriorityDisplayName());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.SfdcAccountID), originalRec.getSfdcAccountID());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.Likelihood), originalRec.getLikelihood());
            Assert.assertEquals(recommendation.get(PlaymakerConstants.MonetaryValue), originalRec.getMonetaryValue());
            Assert.assertEquals(RatingBucketName.valueOf(recommendation.get(PlaymakerConstants.PriorityID).toString()),
                    originalRec.getPriorityID());
            List<Map<String, String>> contactList = PlaymakerUtils
                    .getExpandedContacts(recommendation.get(PlaymakerConstants.Contacts).toString());
            Assert.assertNotNull(contactList);
            Assert.assertTrue(contactList.size() == 1);
            Assert.assertEquals(contactList.get(0).get("Email"), DUMMY_EMAIL);
            Assert.assertEquals(contactList.get(0).get("ZipCode"), DUMMY_ZIP);
        } catch (AssertionError | Exception ex) {
            throw new RuntimeException(
                    String.format("Exception = %s, orgInfo = %s", ex.getMessage(), JsonUtils.serialize(orgInfo)), ex);
        }
    }

    private void compareOriginalAndExtractedRecommendations(Recommendation originalRec, Recommendation recommendation) {
        Map<String, String> orgInfo = null;
        if (StringUtils.isNotBlank(originalRec.getDestinationOrgId())) {
            orgInfo = new HashMap<>();
            orgInfo.put(CDLConstants.ORG_ID, originalRec.getDestinationOrgId());
            orgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, originalRec.getDestinationSysType());
        }

        try {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
            Assert.assertNotNull(recommendation.getContacts());
            Assert.assertEquals(recommendation.getRecommendationId(), originalRec.getRecommendationId());
            Assert.assertEquals(recommendation.getPid(), originalRec.getPid());
            Assert.assertEquals(recommendation.getAccountId(), originalRec.getAccountId());
            Assert.assertEquals(recommendation.getDescription(), originalRec.getDescription());
            Assert.assertEquals(recommendation.getDestinationOrgId(), originalRec.getDestinationOrgId());
            Assert.assertEquals(recommendation.getDestinationSysType(), originalRec.getDestinationSysType());
            Assert.assertEquals(recommendation.getId(), originalRec.getId());
            Assert.assertEquals(recommendation.getLaunchId(), originalRec.getLaunchId());
            Assert.assertEquals(recommendation.getLeAccountExternalID(), originalRec.getLeAccountExternalID());
            Assert.assertEquals(recommendation.getMonetaryValueIso4217ID(), originalRec.getMonetaryValueIso4217ID());
            Assert.assertEquals(recommendation.getPlayId(), originalRec.getPlayId());
            Assert.assertEquals(recommendation.getPriorityDisplayName(), originalRec.getPriorityDisplayName());
            Assert.assertEquals(recommendation.getSfdcAccountID(), originalRec.getSfdcAccountID());
            Assert.assertEquals(recommendation.getSynchronizationDestination(),
                    originalRec.getSynchronizationDestination());
            Assert.assertEquals(recommendation.getLikelihood(), originalRec.getLikelihood());
            Assert.assertEquals(recommendation.getMonetaryValue(), originalRec.getMonetaryValue());
            Assert.assertEquals(recommendation.getPriorityID(), originalRec.getPriorityID());
            Assert.assertEquals(recommendation.getTenantId(), originalRec.getTenantId());
            List<Map<String, String>> contactList = recommendation.getExpandedContacts();
            Assert.assertNotNull(contactList);
            Assert.assertTrue(contactList.size() == 1);
            Assert.assertEquals(contactList.get(0).get("Email"), DUMMY_EMAIL);
            Assert.assertEquals(contactList.get(0).get("ZipCode"), DUMMY_ZIP);
        } catch (AssertionError | Exception ex) {
            throw new RuntimeException(
                    String.format("Exception = %s, orgInfo = %s", ex.getMessage(), JsonUtils.serialize(orgInfo)), ex);
        }
    }
}
