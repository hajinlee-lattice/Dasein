package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.pls.RuleBucketName;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.pls.service.PlayLaunchService;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayLaunchServiceImplTestNG extends PlsFunctionalTestNGBase {

    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private PlayLaunchService playLaunchService;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";
    private Map<String, PlayLaunch> playLaunchMap;

    private Tenant tenant1;

    private Set<RuleBucketName> bucketsToLaunch1;
    private Set<RuleBucketName> bucketsToLaunch2;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        tenant1 = testBed.getTestTenants().get(0);
        setupSecurityContext(tenant1);

        cleanupPlayLunches();

        Date timestamp = new Date(System.currentTimeMillis());

        play = new Play();
        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(tenant1);
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);

        playEntityMgr.create(play);
        play = playEntityMgr.findByName(NAME);

        bucketsToLaunch1 = new TreeSet<>(Arrays.asList(RuleBucketName.values()));

        playLaunch1 = new PlayLaunch();
        playLaunch1.setTenant(tenant1);
        playLaunch1.setLaunchState(LaunchState.Launching);
        playLaunch1.setPlay(play);
        playLaunch1.setBucketsToLaunch(bucketsToLaunch1);

        bucketsToLaunch2 = new TreeSet<>();
        bucketsToLaunch2.add(RuleBucketName.A_MINUS);
        bucketsToLaunch2.add(RuleBucketName.B);

        playLaunch2 = new PlayLaunch();
        playLaunch2.setTenant(tenant1);
        playLaunch2.setLaunchState(LaunchState.Launching);
        playLaunch2.setPlay(play);
        playLaunch2.setBucketsToLaunch(bucketsToLaunch2);
    }

    private void cleanupPlayLunches() {
        for (PlayLaunch launch : playLaunchService.findByState(LaunchState.Launching)) {
            playLaunchService.deleteByLaunchId(launch.getLaunchId());
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch1 != null) {
            playLaunchService.deleteByLaunchId(playLaunch1.getLaunchId());
        }
        if (playLaunch2 != null) {
            playLaunchService.deleteByLaunchId(playLaunch2.getLaunchId());
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
        Thread.sleep(2000);
        playLaunchService.create(playLaunch2);
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
        setupSecurityContext(tenant1);

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

    private void assertBucketsToLaunch(PlayLaunch launch, Set<RuleBucketName> expectedBucketsToLaunch) {
        Set<RuleBucketName> actualBucketsToLaunch = launch.getBucketsToLaunch();
        Assert.assertNotNull(actualBucketsToLaunch);
        Assert.assertTrue(CollectionUtils.isNotEmpty(actualBucketsToLaunch));
        Assert.assertEquals(actualBucketsToLaunch.size(), expectedBucketsToLaunch.size());

        for (RuleBucketName expectedBucket : expectedBucketsToLaunch) {
            Assert.assertTrue(actualBucketsToLaunch.contains(expectedBucket));
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateLaunch" })
    public void testCountDashboard() {
        setupSecurityContext(tenant1);

        Long badPlayId = System.currentTimeMillis();
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

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
    public void testEntriesDashboard() {
        setupSecurityContext(tenant1);

        Long badPlayId = System.currentTimeMillis();
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

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
    public void testDelete() {
        setupSecurityContext(tenant1);

        playLaunchService.deleteByLaunchId(playLaunch1.getLaunchId());
        System.out.println("deleted " + playLaunch1.getLaunchId());
        playLaunchService.deleteByLaunchId(playLaunch2.getLaunchId());
        System.out.println("deleted " + playLaunch2.getLaunchId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch1.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);
        retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);

        checkNonExistance();
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
                endTimestamp);
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
                max, null, true, endTimestamp);

        Assert.assertNotNull(dashboardEntries);
        Assert.assertNotNull(dashboardEntries.getCumulativeStats());
        Assert.assertNotNull(dashboardEntries.getLaunchSummaries());
        Assert.assertEquals(dashboardEntries.getLaunchSummaries().size(), expectedCount.longValue());

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
                        Assert.assertEquals(stats.getErrors(), matchingPlayLaunch.getAccountsErrored().longValue());
                        Assert.assertEquals(stats.getRecommendationsLaunched(),
                                matchingPlayLaunch.getAccountsLaunched().longValue());
                        Assert.assertEquals(stats.getSuppressed(),
                                matchingPlayLaunch.getAccountsSuppressed().longValue());
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
        }
    }

    private void checkNonExistance() {
        setupSecurityContext(tenant1);
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
