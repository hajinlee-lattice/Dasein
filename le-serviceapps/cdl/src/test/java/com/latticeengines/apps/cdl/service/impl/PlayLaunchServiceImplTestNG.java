package com.latticeengines.apps.cdl.service.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.service.DataIntegrationStatusMonitoringService;
import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.service.PlayLaunchService;
import com.latticeengines.apps.cdl.service.PlayService;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationEventType;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitor;
import com.latticeengines.domain.exposed.cdl.DataIntegrationStatusMonitorMessage;
import com.latticeengines.domain.exposed.cdl.MessageType;
import com.latticeengines.domain.exposed.cdl.ProgressEventDetail;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannelMap;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayLaunchServiceImplTestNG extends CDLFunctionalTestNGBase {

    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private PlayLaunchService playLaunchService;

    @Inject
    private PlayService playService;

    @Inject
    private TenantService tenantService;

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private DataIntegrationStatusMonitoringService dataIntegrationStatusMonitoringService;

    private Play play;

    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;

    String org1 = "org1_" + CURRENT_TIME_MILLIS;
    String org2 = "org2_" + CURRENT_TIME_MILLIS;

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String PLAY_TARGET_SEGMENT_NAME = "Play Target Segment - 2";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private Map<String, PlayLaunch> playLaunchMap;
    private List<PlayType> playTypes;
    private Set<RatingBucketName> bucketsToLaunch1;
    private Set<RatingBucketName> bucketsToLaunch2;
    private MetadataSegment playTargetSegment;
    private LookupIdMap lookupIdMap1;
    private LookupIdMap lookupIdMap2;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithDummySegment();

        cleanupPlayLaunches();

        Date timestamp = new Date(System.currentTimeMillis());

        playTypes = playTypeService.getAllPlayTypes(mainCustomerSpace);
        playTargetSegment = createMetadataSegment(PLAY_TARGET_SEGMENT_NAME);
        assertNotNull(playTargetSegment);
        assertEquals(playTargetSegment.getDisplayName(), PLAY_TARGET_SEGMENT_NAME);

        play = new Play();
        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(mainTestTenant);
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(playTargetSegment);
        play.setPlayType(playTypes.get(0));

        playEntityMgr.create(play);
        play = playEntityMgr.getPlayByName(NAME, false);
        assertPlayTargetSegment(play);

        bucketsToLaunch1 = new TreeSet<>(Arrays.asList(RatingBucketName.values()));

        playLaunch1 = new PlayLaunch();
        playLaunch1.setLaunchId(NamingUtils.randomSuffix("pl", 16));
        playLaunch1.setTenant(mainTestTenant);
        playLaunch1.setLaunchState(LaunchState.Launching);
        playLaunch1.setPlay(play);
        playLaunch1.setBucketsToLaunch(bucketsToLaunch1);
        playLaunch1.setDestinationAccountId("SFDC_ACC1");
        playLaunch1.setDestinationOrgId(org1);
        playLaunch1.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch1.setCreatedBy(CREATED_BY);
        playLaunch1.setUpdatedBy(CREATED_BY);

        bucketsToLaunch2 = new TreeSet<>();
        bucketsToLaunch2.add(RatingBucketName.A);
        bucketsToLaunch2.add(RatingBucketName.B);

        playLaunch2 = new PlayLaunch();
        playLaunch2.setLaunchId(NamingUtils.randomSuffix("pl", 16));
        playLaunch2.setTenant(mainTestTenant);
        playLaunch2.setLaunchState(LaunchState.Launching);
        playLaunch2.setPlay(play);
        playLaunch2.setBucketsToLaunch(bucketsToLaunch2);
        playLaunch2.setDestinationAccountId("SFDC_ACC2");
        playLaunch2.setDestinationOrgId(org2);
        playLaunch2.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch2.setCreatedBy(CREATED_BY);
        playLaunch2.setUpdatedBy(CREATED_BY);

        lookupIdMap1 = new LookupIdMap();
        lookupIdMap1.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap1.setExternalSystemName(CDLExternalSystemName.Salesforce);
        lookupIdMap1.setOrgId(org1);
        lookupIdMap1.setOrgName("org2name");

        lookupIdMap2 = new LookupIdMap();
        lookupIdMap2.setExternalSystemType(CDLExternalSystemType.CRM);
        lookupIdMap2.setExternalSystemName(CDLExternalSystemName.Salesforce);
        lookupIdMap2.setOrgId(org2);
        lookupIdMap2.setOrgName("org1name");

    }

    private DataIntegrationStatusMonitorMessage constructDefaultStatusMessage(String workflowRequestId,
            DataIntegrationEventType eventType, PlayLaunch pl) {
        DataIntegrationStatusMonitorMessage statusMessage = new DataIntegrationStatusMonitorMessage();
        statusMessage.setTenantName(mainTestTenant.getName());
        statusMessage.setWorkflowRequestId(workflowRequestId);
        statusMessage.setEntityId(pl.getLaunchId());
        statusMessage.setEntityName("PlayLaunch");
        statusMessage.setExternalSystemId(UUID.randomUUID().toString());
        statusMessage.setOperation("export");
        statusMessage.setMessageType(MessageType.Event.toString());
        statusMessage.setMessage("This workflow has been submitted");
        statusMessage.setEventType(eventType.toString());
        statusMessage.setEventTime(new Date());
        statusMessage.setSourceFile("dropfolder/source.csv");
        return statusMessage;
    }

    private void assertPlayTargetSegment(Play testPlay) {
        assertNotNull(testPlay.getTargetSegment());
        assertEquals(testPlay.getTargetSegment().getDisplayName(), PLAY_TARGET_SEGMENT_NAME);
    }

    private void cleanupPlayLaunches() {
        for (PlayLaunch launch : playLaunchService.findByState(LaunchState.Launching)) {
            playLaunchService.deleteByLaunchId(launch.getLaunchId(), false);
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch1 != null) {
            playLaunchService.deleteByLaunchId(playLaunch1.getLaunchId(), false);
        }
        if (playLaunch2 != null) {
            playLaunchService.deleteByLaunchId(playLaunch2.getLaunchId(), false);
        }
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        checkNonExistance();
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testCreateLaunch() throws InterruptedException {
        playLaunchService.create(playLaunch1);
        Thread.sleep(1000);
        playLaunchService.create(playLaunch2);
        Thread.sleep(1000);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateLaunch" })
    public void testBasicOperations() {
        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNotNull(retreivedPlayLaunch);

        List<LaunchState> states = new ArrayList<>();
        states.add(LaunchState.Launched);
        List<PlayLaunch> playLaunchList = playLaunchService.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);

        states.add(LaunchState.Launching);
        playLaunchList = playLaunchService.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 2);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        playLaunchList = playLaunchService.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);

        Assert.assertEquals(playLaunchList.size(), 2);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        retreivedPlayLaunch = playLaunchService.findLatestByPlayId(play.getPid(), states);
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getPid(), playLaunch2.getPid());

        retreivedPlayLaunch = playLaunchService.findLatestByPlayId(play.getPid(), null);
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getPid(), playLaunch2.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testUpdateLaunch() throws InterruptedException {

        playLaunch1 = playLaunchService.findByLaunchId(playLaunch1.getLaunchId());
        assertBucketsToLaunch(playLaunch1, bucketsToLaunch1);

        playLaunch1.setLaunchState(LaunchState.Launched);
        playLaunch1.setAccountsErrored(1L);
        playLaunch1.setAccountsLaunched(5L);
        playLaunch1.setAccountsSuppressed(3L);
        playLaunch1.setContactsLaunched(7L);

        playLaunch2 = playLaunchService.findByLaunchId(playLaunch2.getLaunchId());
        assertBucketsToLaunch(playLaunch2, bucketsToLaunch2);

        playLaunch2.setLaunchState(LaunchState.Launched);
        playLaunch2.setAccountsErrored(2L);
        playLaunch2.setAccountsLaunched(10L);
        playLaunch2.setAccountsSuppressed(5L);
        playLaunch2.setContactsLaunched(8L);

        playLaunch1 = playLaunchService.update(playLaunch1);
        playLaunch2 = playLaunchService.update(playLaunch2);

        playLaunchMap = new HashMap<>();

        playLaunchMap.put(playLaunch1.getId(), playLaunch1);
        playLaunchMap.put(playLaunch2.getId(), playLaunch2);

        Thread.sleep(2000);
    }

    private void assertBucketsToLaunch(PlayLaunch launch, Set<RatingBucketName> expectedBucketsToLaunch) {
        Set<RatingBucketName> actualBucketsToLaunch = launch.getBucketsToLaunch();
        Assert.assertNotNull(actualBucketsToLaunch);
        Assert.assertTrue(CollectionUtils.isNotEmpty(actualBucketsToLaunch));
        Assert.assertEquals(actualBucketsToLaunch.size(), expectedBucketsToLaunch.size());

        for (RatingBucketName expectedBucket : expectedBucketsToLaunch) {
            Assert.assertTrue(actualBucketsToLaunch.contains(expectedBucket));
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateLaunch" })
    public void testGetLaunchConfigurations() {
        lookupIdMappingService.registerExternalSystem(lookupIdMap1);
        lookupIdMappingService.registerExternalSystem(lookupIdMap2);

        PlayLaunchChannelMap channelMap = playLaunchService.getPlayLaunchChannelMap(play.getName());
        Assert.assertNotNull(channelMap);
        Map<String, PlayLaunchChannel> configurationMap = channelMap.getLaunchChannelMap();
        Assert.assertEquals(configurationMap.get(org1).getPlayLaunch().getPid(), playLaunch1.getPid());
        Assert.assertEquals(configurationMap.get(org2).getPlayLaunch().getPid(), playLaunch2.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetLaunchConfigurations" })
    public void testCountDashboard() {

        Long badPlayId = System.currentTimeMillis();
        List<LaunchState> goodStates = Arrays.asList(LaunchState.Launched, LaunchState.Launching);
        List<LaunchState> badStates = Arrays.asList(LaunchState.Failed, LaunchState.Launching);

        checkCountForDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L);

        checkCountForDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L);

        checkCountForDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L);

        checkCountForDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L);

        checkCountForDashboard(null, goodStates, badStates, playLaunch1.getCreated().getTime() - 1000L,
                playLaunch2.getCreated().getTime() - 1000L, 1L, 2L);

        checkCountForDashboard(null, goodStates, badStates, playLaunch1.getCreated().getTime() + 1000L,
                playLaunch2.getCreated().getTime() + 1000L, 1L, 1L);

        checkCountForDashboard(null, goodStates, badStates, playLaunch1.getCreated().getTime() - 1000L,
                playLaunch2.getCreated().getTime() + 1000L, 2L, 2L);

        checkCountForDashboard(null, goodStates, badStates, playLaunch1.getCreated().getTime(),
                playLaunch2.getCreated().getTime(), 2L, 2L);

        checkCountForDashboard(null, goodStates, badStates, 0L, playLaunch1.getCreated().getTime() - 1000L, 0L, 2L);

        checkCountForDashboard(null, goodStates, badStates, playLaunch2.getCreated().getTime() + 1000L,
                System.currentTimeMillis(), 0L, 0L);

        checkCountForDashboard(null, goodStates, badStates, 0L, 1L, 0L, 2L);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCountDashboard" })
    public void testCreateExportIntegrationStatusMonitor() throws InterruptedException {
        // Simulate workflow Launch and external integration with Tray Export
        // Flow
        DataIntegrationStatusMonitorMessage statusMessage1 = constructDefaultStatusMessage(UUID.randomUUID().toString(),
                DataIntegrationEventType.WorkflowSubmitted, playLaunch1);
        dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage1);
        DataIntegrationStatusMonitorMessage statusMessage2 = constructDefaultStatusMessage(UUID.randomUUID().toString(),
                DataIntegrationEventType.WorkflowSubmitted, playLaunch2);
        dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage2);

        Thread.sleep(2000L);
        List<DataIntegrationStatusMonitor> statusMonitorList = dataIntegrationStatusMonitoringService
                .getAllStatuses(mainTestTenant.getId());
        assertNotNull(statusMonitorList);
        assertEquals(statusMonitorList.size(), 2);

        statusMessage2.setEventType(DataIntegrationEventType.Completed.toString());
        ProgressEventDetail eventDetail = new ProgressEventDetail();
        eventDetail.setFailed(0L);
        eventDetail.setProcessed(1L);
        eventDetail.setTotalRecordsSubmitted(1L);
        statusMessage2.setEventDetail(eventDetail);
        statusMessage2.setMessage("Workflow marked as Complete");
        dataIntegrationStatusMonitoringService.createOrUpdateStatus(statusMessage2);
        Thread.sleep(1000L);

        playLaunch2 = playLaunchService.findByLaunchId(playLaunch2.getLaunchId());

        assertEquals(playLaunch2.getLaunchState(), LaunchState.Synced);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateExportIntegrationStatusMonitor" })
    public void testEntriesDashboard() {
        Long badPlayId = System.currentTimeMillis();
        List<LaunchState> goodStates = Arrays.asList(LaunchState.Launched, LaunchState.Launching, LaunchState.Synced,
                LaunchState.Syncing);
        List<LaunchState> badStates = Arrays.asList(LaunchState.Failed, LaunchState.Launching, LaunchState.Syncing);

        checkForEntriesDashboard(badPlayId, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 0L, 0L);

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 2L, 2L);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 2L, 2L);

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L);

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L);

        checkForEntriesDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1), 0L, 10L,
                System.currentTimeMillis(), 0L, 0L);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, 1L, 0L, 2L);
    }

    @Test(groups = "functional", dependsOnMethods = { "testEntriesDashboard" })
    public void testBasicDelete() {

        playLaunchService.deleteByLaunchId(playLaunch1.getLaunchId(), false);
        playLaunchService.deleteByLaunchId(playLaunch2.getLaunchId(), false);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(2L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch1.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);
        retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);

        checkNonExistance();
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicDelete" })
    public void testDeleteViaPlay() {
        PlayLaunch playLaunch3 = new PlayLaunch();
        playLaunch3.setTenant(mainTestTenant);
        playLaunch3.setLaunchState(LaunchState.Launching);
        playLaunch3.setPlay(play);
        playLaunch3.setBucketsToLaunch(bucketsToLaunch2);
        playLaunch3.setDestinationAccountId("SFDC_ACC2");
        playLaunch3.setDestinationOrgId(org2);
        playLaunch3.setDestinationSysType(CDLExternalSystemType.CRM);
        playLaunch3.setCreatedBy(CREATED_BY);
        playLaunch3.setUpdatedBy(CREATED_BY);
        playLaunchService.create(playLaunch3);

        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch3.getLaunchId());
        Assert.assertNotNull(retreivedPlayLaunch);

        playService.deleteByName(play.getName(), false);
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(2L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch3.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);
    }

    private void checkCountForDashboard(Long playId, List<LaunchState> goodStates, List<LaunchState> badStates,
            Long startTimestamp, Long endTimestamp, long expectedCount, long expectedCountForNullEndTimestmp) {
        checkCountForDashboard(playId, goodStates, startTimestamp, endTimestamp, expectedCount);
        checkCountForDashboard(playId, null, startTimestamp, endTimestamp, expectedCount);
        checkCountForDashboard(playId, goodStates, startTimestamp, null, expectedCountForNullEndTimestmp);
        checkCountForDashboard(playId, null, startTimestamp, null, expectedCountForNullEndTimestmp);
        checkCountForDashboard(playId, badStates, startTimestamp, endTimestamp, 0);
    }

    private void checkCountForDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp,
            long expectedCount) {
        Long dashboardEntriesCount = playLaunchService.getDashboardEntriesCount(playId, states, startTimestamp,
                endTimestamp, null, null);
        Assert.assertNotNull(dashboardEntriesCount);
        Assert.assertEquals(dashboardEntriesCount.longValue(), expectedCount);
    }

    private void checkForEntriesDashboard(Long playId, List<LaunchState> goodStates, List<LaunchState> badStates,
            Long startTimestamp, Long offset, Long max, Long endTimestamp, Long expectedCount,
            long expectedCountForNullEndTimestmp) {
        checkForEntriesDashboard(playId, goodStates, startTimestamp, offset, max, endTimestamp, expectedCount);
        checkForEntriesDashboard(playId, null, startTimestamp, offset, max, endTimestamp, expectedCount);
        checkForEntriesDashboard(playId, goodStates, startTimestamp, offset, max, null,
                expectedCountForNullEndTimestmp);
        checkForEntriesDashboard(playId, null, startTimestamp, offset, max, null, expectedCountForNullEndTimestmp);
        checkForEntriesDashboard(playId, badStates, startTimestamp, offset, max, endTimestamp, 0L);
    }

    private void checkForEntriesDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, Long endTimestamp, Long expectedCount) {
        PlayLaunchDashboard dashboardEntries = playLaunchService.getDashboard(playId, states, startTimestamp, offset,
                max, null, true, endTimestamp, null, null, true);

        Set<String> orgSet = new HashSet<>(Arrays.asList(org1, org2));

        Assert.assertNotNull(dashboardEntries);
        Assert.assertNotNull(dashboardEntries.getCumulativeStats());
        Assert.assertNotNull(dashboardEntries.getLaunchSummaries());
        Assert.assertEquals(dashboardEntries.getLaunchSummaries().size(), expectedCount.longValue());
        Assert.assertNotNull(dashboardEntries.getUniqueLookupIdMapping());

        Set<String> launchIds = new HashSet<>();
        launchIds.add(playLaunch1.getId());
        launchIds.add(playLaunch2.getId());

        if (dashboardEntries.getLaunchSummaries().size() > 0) {
            dashboardEntries.getLaunchSummaries().stream() //
                    .forEach(entry -> {
                        Assert.assertNotNull(entry.getLaunchId());
                        Assert.assertTrue(launchIds.contains(entry.getLaunchId()));
                        Assert.assertNotNull(entry.getLaunchState());
                        Assert.assertNotNull(entry.getLaunchTime());
                        Assert.assertNotNull(entry.getPlayName());
                        Assert.assertNotNull(entry.getPlayDisplayName());
                        Assert.assertNotNull(entry.getSelectedBuckets());
                        Stats stats = entry.getStats();
                        Assert.assertNotNull(stats);

                        PlayLaunch matchingPlayLaunch = playLaunchMap.get(entry.getLaunchId());
                        Assert.assertEquals(stats.getContactsWithinRecommendations(),
                                matchingPlayLaunch.getContactsLaunched().longValue());
                        Assert.assertEquals(stats.getAccountErrors(),
                                matchingPlayLaunch.getAccountsErrored().longValue());
                        Assert.assertEquals(stats.getRecommendationsLaunched(),
                                matchingPlayLaunch.getAccountsLaunched().longValue());
                        Assert.assertEquals(stats.getSuppressed(),
                                matchingPlayLaunch.getAccountsSuppressed().longValue());
                        Assert.assertNotNull(entry.getDestinationOrgId());
                        Assert.assertNotNull(entry.getDestinationSysType());
                        Assert.assertTrue(orgSet.contains(entry.getDestinationOrgId()));
                        Assert.assertNotNull(entry.getDestinationAccountId());

                        // Check for DataIntegrationStatusMonitor
                        assertNotNull(entry.getIntegrationStatusMonitor());
                        assertEquals(entry.getIntegrationStatusMonitor().getEntityId(), entry.getLaunchId());
                    });

            Set<String> playIdSet = ConcurrentHashMap.newKeySet();

            Assert.assertNotNull(dashboardEntries.getUniquePlaysWithLaunches());
            dashboardEntries.getUniquePlaysWithLaunches().stream() //
                    .forEach(pl -> {
                        Assert.assertNotNull(pl.getPid());
                        Assert.assertNotNull(pl.getName());
                        Assert.assertNotNull(pl.getDisplayName());
                        Assert.assertFalse(playIdSet.contains(pl.getName()));
                        playIdSet.add(pl.getName());
                    });

            if (expectedCount > 0) {
                Assert.assertEquals(dashboardEntries.getUniqueLookupIdMapping().size(), 1);
                Assert.assertEquals(
                        dashboardEntries.getUniqueLookupIdMapping().get(CDLExternalSystemType.CRM.name()).size(),
                        orgSet.size());
                dashboardEntries.getUniqueLookupIdMapping() //
                        .get(CDLExternalSystemType.CRM.name()).stream() //
                        .forEach(lookupConfig -> {
                            Assert.assertNotNull(lookupConfig.getOrgId());
                            Assert.assertEquals(lookupConfig.getExternalSystemType(), CDLExternalSystemType.CRM);
                            Assert.assertTrue(orgSet.contains(lookupConfig.getOrgId()));
                        });
            }
        }
    }

    private void checkNonExistance() {
        List<LaunchState> states = new ArrayList<>();
        states.add(LaunchState.Launching);
        List<PlayLaunch> playLaunchList = playLaunchService.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);

        playLaunchList = playLaunchService.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);
    }
}
