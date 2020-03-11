package com.latticeengines.playmaker.service.impl;

import static org.testng.Assert.assertEquals;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.SleepUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.playmaker.PlaymakerConstants;
import com.latticeengines.domain.exposed.playmaker.PlaymakerSyncLookupSource;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmaker.dao.impl.LpiPMRecommendationDaoAdapterImpl;
import com.latticeengines.playmaker.entitymgr.PlaymakerRecommendationEntityMgr;
import com.latticeengines.playmaker.service.LpiPMAccountExtension;
import com.latticeengines.playmakercore.entitymanager.RecommendationEntityMgr;
import com.latticeengines.playmakercore.service.LpiPMRecommendation;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.GlobalAuthDeploymentTestBed;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml",
        "classpath:playmakercore-context.xml", "classpath:test-playmaker-context.xml" })
public class LpiPMRecommendationImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private RecommendationEntityMgr recommendationEntityMgr;

    @Inject
    private PlaymakerRecommendationEntityMgr playmakerRecommendationMgr;

    @Inject
    private LpiPMRecommendation lpiPMRecommendation;

    @Inject
    private LpiPMAccountExtension lpiAccountExt;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private LpiPMRecommendationDaoAdapterImpl lpiReDaoAdapter;

    @Value("${playmaker.recommendations.years.keep:2}")
    private Double YEARS_TO_KEEP_RECOMMENDATIONS;

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private CustomerSpace customerSpace;

    private Map<String, String> eloquaAppId1;
    private Map<String, String> eloquaAppId2;
    private Map<String, String> badOrgInfo;

    private int maxUpdateRows = 20;
    private String syncDestination = "SFDC";
    private Map<String, String> orgInfo;
    private TestPlaySetupConfig testPlaySetupConfig;

    private GlobalAuthDeploymentTestBed testBed;
    private int[] recommendationCounts = new int[]{10, 20, 30, 40, 50};

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        badOrgInfo = new HashMap<>();
        badOrgInfo.put(CDLConstants.ORG_ID, "BAD_ID_" + System.currentTimeMillis());
        badOrgInfo.put(CDLConstants.EXTERNAL_SYSTEM_TYPE, "CRM");
        eloquaAppId1 = new HashMap<>();
        eloquaAppId2 = new HashMap<>();
        eloquaAppId1.put(CDLConstants.AUTH_APP_ID, "lattice.eloqua01234");
        eloquaAppId2.put(CDLConstants.AUTH_APP_ID, "BIS01234");
        String testOrgName = CDLExternalSystemName.Salesforce.name() + System.currentTimeMillis();
        testPlaySetupConfig = new TestPlaySetupConfig.Builder() //
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM) //
                        .destinationSystemName(CDLExternalSystemName.Salesforce) //
                        .destinationSystemId(testOrgName) //
                        .build()).build();
        testPlayCreationHelper.setupTenantAndCreatePlay(testPlaySetupConfig);
        tenant = testPlayCreationHelper.getTenant();
        customerSpace = CustomerSpace.parse(tenant.getId());
        play = testPlayCreationHelper.getPlay();
        playLaunch = testPlayCreationHelper.getPlayLaunch();
        orgInfo = getOrgInfo();
        updatePlayLaunchData(tenant, play, playLaunch, maxUpdateRows * 2);
        for (int recCount : recommendationCounts) {
            SleepUtils.sleep(2000L);
            PlayLaunch playLaunch = testPlayCreationHelper.createPlayLaunch2(testPlaySetupConfig);
            updatePlayLaunchData(tenant, play, playLaunch, recCount);
        }
        // make sure tenant will clear
        testBed = testPlayCreationHelper.getDeploymentTestBed();
    }

    private void updatePlayLaunchData(Tenant tenant, Play play, PlayLaunch playLaunch, int recCount) {
        playProxy.updatePlayLaunch(tenant.getId(), play.getName(), playLaunch.getLaunchId(), LaunchState.Launching);
        Long count = createDummyRecommendations(recCount, playLaunch);
        playProxy.updatePlayLaunchProgress(tenant.getId(), play.getName(), playLaunch.getLaunchId(), 100.0, count,
                count, 0L, 0L, 0L, 0L);
        playProxy.updatePlayLaunch(tenant.getId(), play.getName(), playLaunch.getLaunchId(), LaunchState.Launched);
    }

    private Map<String, String> getOrgInfo() {
        Map<String, String> org = new HashMap<>();
        org.put(CDLConstants.ORG_ID, testPlaySetupConfig.getSinglePlayLaunchChannelConfig().getDestinationSystemId());
        org.put(CDLConstants.EXTERNAL_SYSTEM_TYPE,
                testPlaySetupConfig.getSinglePlayLaunchChannelConfig().getDestinationSystemType().toString());
        return org;
    }

    @Test(groups = "deployment")
    public void testRecommendations() {
        List<Recommendation> recommendations = recommendationEntityMgr.findRecommendations(new Date(0), 0, maxUpdateRows * 10, //
                syncDestination, null, orgInfo);
        Assert.assertTrue(CollectionUtils.isNotEmpty(recommendations));
        int count = recommendations.size();
        int expectedCount = maxUpdateRows * 2 + Arrays.stream(recommendationCounts).sum();
        Assert.assertEquals(count, expectedCount);
        List<Map<String, Object>> recommendations2 = lpiPMRecommendation.getRecommendations(0, 0, count, //
                SynchronizationDestinationEnum.SFDC, null, orgInfo, eloquaAppId2);
        Assert.assertNotNull(recommendations2);
        Assert.assertEquals(count, recommendations2.size());
        long start = 0;
        validateRecommendations(start, 20, 45, 0, null, orgInfo, eloquaAppId2, 20, 3, 45);
        validateRecommendations(start, 40, 60, 0, null, orgInfo, eloquaAppId2, 0, 3, 60);
        validateRecommendations(start, 50, 5, 0, null, orgInfo, eloquaAppId2, 0, 1, 5);
        validateRecommendations(start, 60, 1000, 0, null, orgInfo, eloquaAppId2, 10, 4, 130);
    }

    private void validateRecommendations(long start, int offset, int maximum, int syncDestination, List<String> playIds,
                                         Map<String, String> orgInfo, Map<String, String> appId, int queryOffset, int queryIdSize, int recommendationSize) {
        Pair<Long, List<String>> queryOffsetAndIds = lpiReDaoAdapter.getQueryOffsetAndIds(start, offset, maximum, syncDestination, playIds, orgInfo, appId);
        Assert.assertEquals(queryOffsetAndIds.getLeft().intValue(), queryOffset);
        Assert.assertEquals(queryOffsetAndIds.getRight().size(), queryIdSize);
        List<Map<String, Object>> result = lpiReDaoAdapter.getRecommendations(start, offset, maximum, syncDestination, playIds, orgInfo, appId);
        Assert.assertEquals(result.size(), recommendationSize);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRecommendations" })
    public void testGetLastLaunchedRecommendation() {
        List<Map<String, Object>> result = lpiReDaoAdapter.getRecommendations(0, 0, 1000, 0, null, orgInfo,
                eloquaAppId1);
        logger.info("Last Launched Recommendations Count: " + result.size());
        logger.info("Last Launched Recommendations: " + result);
        Assert.assertTrue(result.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRecommendations" })
    public void testGetAccountIdsByLaunchIds() {
        List<Map<String, Object>> accounts = lpiAccountExt.getAccountIdsByRecommendationsInfo(false, 0L, 0L, 1000L,
                orgInfo);
        System.out.println("\nThis is AccountID List:");
        System.out.println(accounts.toString() + "\n");
        Assert.assertTrue(accounts.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRecommendations" })
    public void testCDLupdatetimeBasedAccountExt() {
        String columns = "AccountID";
        List<Map<String, Object>> accounts = lpiAccountExt.getAccountExtensions(0, 0, 1000, null, null, 0L, columns,
                true, orgInfo);
        System.out.println("\nThis is CDL_TIME based account List:");
        System.out.println(accounts.toString() + "\n");
        Assert.assertTrue(accounts.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRecommendations" })
    public void testRecommendationBasedAccountExt() {
        String columns = "AccountID";
        List<Map<String, Object>> accounts = lpiAccountExt.getAccountExtensions(0, 0, 1000, null, null, 1L, columns,
                true, orgInfo);
        System.out.println("\nThis is Recommendation based account List:");
        System.out.println(accounts.toString() + "\n");
        Assert.assertTrue(accounts.size() > 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetLastLaunchedRecommendation" })
    public void testGetContacts() throws Exception {
        // Get Contacts with out play filter
        List<Map<String, Object>> contacts = lpiReDaoAdapter.getContacts(0, 0, 1000, null, null, 140000000L, null,
                orgInfo, eloquaAppId1);
        Assert.assertNotNull(contacts);
        long count = contacts.size();
        logger.info("Contact Count: " + count);
        Assert.assertTrue(count >= maxUpdateRows);
        long contactCount = lpiReDaoAdapter.getContactCount(0, null, null, 140000000L, null, orgInfo, eloquaAppId1);
        assertEquals(contactCount, count);

        // Get Contacts with play filter
        List<Map<String, Object>> contactsWithPlayFilter = lpiReDaoAdapter.getContacts(0, 0, 1000, null, null,
                140000000L, Arrays.asList(play.getName()), orgInfo, eloquaAppId1);
        Assert.assertNotNull(contactsWithPlayFilter);
        assertEquals(contactsWithPlayFilter.size(), contacts.size());
        contactCount = lpiReDaoAdapter.getContactCount(0, null, null, 140000000L, Arrays.asList(play.getName()),
                orgInfo, eloquaAppId1);
        assertEquals(contactCount, contactsWithPlayFilter.size());

        // Get Contacts with dummy play filter
        List<Map<String, Object>> contactsWithDummyPlayFilter = lpiReDaoAdapter.getContacts(0, 0, 1000, null, null,
                140000000L, Arrays.asList("DUMMY"), orgInfo, eloquaAppId1);
        Assert.assertNotNull(contactsWithDummyPlayFilter);
        assertEquals(contactsWithDummyPlayFilter.size(), 0);
        contactCount = lpiReDaoAdapter.getContactCount(0, null, null, 140000000L, Arrays.asList("DUMMY"), orgInfo,
                eloquaAppId1);
        assertEquals(contactCount, contactsWithDummyPlayFilter.size());
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() throws Exception {
        testPlayCreationHelper.cleanupArtifacts(true);
    }

    @Test(groups = "deployment")
    public void testPlays() {
        Map<String, Object> playCount = playmakerRecommendationMgr.getPlayCount(customerSpace.toString(),
                PlaymakerSyncLookupSource.V2.name(), 0, null, SynchronizationDestinationEnum.SFDC.ordinal(),
                badOrgInfo);
        Assert.assertNotNull(playCount);
        Object countObj = playCount.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertNotNull(countObj);
        Long count = (Long) countObj;
        Assert.assertNotNull(count);
        Assert.assertTrue(count == 0);

        playCount = playmakerRecommendationMgr.getPlayCount(customerSpace.toString(),
                PlaymakerSyncLookupSource.V2.name(), 0, null, SynchronizationDestinationEnum.SFDC.ordinal(), orgInfo);
        Assert.assertNotNull(playCount);
        countObj = playCount.get(PlaymakerRecommendationEntityMgr.COUNT_KEY);
        Assert.assertNotNull(countObj);
        count = (Long) countObj;
        Assert.assertNotNull(count);
        Assert.assertTrue(count > 0);
        // not actual restriction - only for this test scenario
        Assert.assertTrue(count < 100);

        Map<String, Object> plays = playmakerRecommendationMgr.getPlays(customerSpace.toString(),
                PlaymakerSyncLookupSource.V2.name(), 0, 0, 10, null, SynchronizationDestinationEnum.SFDC.ordinal(),
                orgInfo);
        Assert.assertNotNull(plays);
        Assert.assertNotNull(plays.get(PlaymakerRecommendationEntityMgr.START_KEY));
        Assert.assertNotNull(plays.get(PlaymakerRecommendationEntityMgr.END_KEY));
        Assert.assertNotNull(plays.get(PlaymakerRecommendationEntityMgr.RECORDS_KEY));
        @SuppressWarnings({ "unchecked" })
        List<Map<String, Object>> result = (List<Map<String, Object>>) plays
                .get(PlaymakerRecommendationEntityMgr.RECORDS_KEY);
        Assert.assertNotNull(result);
        Assert.assertEquals(result.size(), count.intValue());
        Assert.assertTrue(CollectionUtils.isNotEmpty(result));
        result.stream() //
                .forEach(playMap -> {
                    Assert.assertNotNull(playMap);
                    Assert.assertTrue(MapUtils.isNotEmpty(playMap));
                    Assert.assertNotNull(playMap.get(PlaymakerConstants.ID));
                    Assert.assertNotNull(playMap.get(PlaymakerConstants.ID + PlaymakerConstants.V2));
                    Assert.assertNotNull(playMap.get(PlaymakerConstants.ExternalId));
                    Assert.assertNotNull(playMap.get(PlaymakerConstants.DisplayName));
                    Assert.assertNotNull(playMap.get(PlaymakerConstants.RowNum));
                });
    }

    private long createDummyRecommendations(int newRecommendationsCount, PlayLaunch playLaunch) {
        Date launchDate = playLaunch.getCreated();
        for (int i = 0; i < newRecommendationsCount; i++) {
            Recommendation rec = new Recommendation();
            rec.setAccountId("Acc_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setCompanyName("CN_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setDeleted(false);
            rec.setDestinationOrgId(orgInfo.get(CDLConstants.ORG_ID));
            rec.setDestinationSysType(orgInfo.get(CDLConstants.EXTERNAL_SYSTEM_TYPE));
            rec.setId("ID_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setLaunchDate(launchDate);
            rec.setLastUpdatedTimestamp(launchDate);
            rec.setLaunchId(playLaunch.getId());
            rec.setLeAccountExternalID("Acc_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setPlayId(play.getName());
            rec.setRecommendationId("ID_" + launchDate.toInstant().toEpochMilli() + "_" + newRecommendationsCount);
            rec.setSynchronizationDestination(syncDestination);
            rec.setTenantId(tenant.getPid());
            String contactStr = "[{\"Email\":\"FirstName5763@com\",  \"Address\": \"null Dr\",  \"Phone\":\"248.813.2000\",\"State\":\"MI\",\"ZipCode\":\"48098-2815\","
                    + "\"ContactID\":\"" + String.valueOf(launchDate.toInstant().toEpochMilli()) + "\","
                    + "\"Country\":\"USA\",\"SfdcContactID\": \"\",\"City\": \"Troy\",\"ContactID\": \"5763\",\"Name\": \"FirstName5763 LastName5763\"}]";
            rec.setContacts(contactStr);
            recommendationEntityMgr.create(rec);
        }
        return newRecommendationsCount;
    }
}

