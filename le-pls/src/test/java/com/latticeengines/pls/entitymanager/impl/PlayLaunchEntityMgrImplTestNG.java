package com.latticeengines.pls.entitymanager.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.LaunchSummary;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.PlayEntityMgr;
import com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayLaunchEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";

    private Tenant tenant1;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithGATenants(1);
        tenant1 = testBed.getTestTenants().get(0);
        setupSecurityContext(tenant1);

        cleanupPlayLunches();

        play = new Play();
        play.setName(NAME);
        play.setDisplayName(DISPLAY_NAME);
        play.setTenant(tenant1);
        Date timestamp = new Date(System.currentTimeMillis());
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);

        playEntityMgr.create(play);
        play = playEntityMgr.findByName(NAME);

        playLaunch1 = new PlayLaunch();
        playLaunch1.setTenant(tenant1);
        playLaunch1.setLaunchState(LaunchState.Launching);
        playLaunch1.setPlay(play);

        playLaunch2 = new PlayLaunch();
        playLaunch2.setTenant(tenant1);
        playLaunch2.setLaunchState(LaunchState.Launching);
        playLaunch2.setPlay(play);
    }

    private void cleanupPlayLunches() {
        for (PlayLaunch launch : playLaunchEntityMgr.findByState(LaunchState.Launching)) {
            playLaunchEntityMgr.deleteByLaunchId(launch.getLaunchId());
        }
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        checkNonExistance();
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testCreateLaunch() throws InterruptedException {
        playLaunchEntityMgr.create(playLaunch1);
        Thread.sleep(2000);
        playLaunchEntityMgr.create(playLaunch2);
        long playLaunch1Pid = playLaunch1.getPid();
        long playLaunch2Pid = playLaunch2.getPid();
        Assert.assertTrue(playLaunch1Pid >= PlayLaunch.PID_INIT_VALUE);
        Assert.assertTrue(playLaunch2Pid > playLaunch1Pid);
        Assert.assertNotNull(playLaunch1.getLaunchId());
        Assert.assertNotNull(playLaunch2.getLaunchId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateLaunch" })
    public void testBasicOperations() {
        setupSecurityContext(tenant1);

        PlayLaunch retreivedPlayLaunch = playLaunchEntityMgr.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNotNull(retreivedPlayLaunch);
        List<LaunchState> states = new ArrayList<>();
        states.add(LaunchState.Launched);
        states.add(LaunchState.Failed);

        List<PlayLaunch> playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);

        List<LaunchState> states1 = new ArrayList<>();
        states1.add(LaunchState.Launching);

        playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states1);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 2);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        states.add(LaunchState.Launching);

        playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 2);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        playLaunchList = playLaunchEntityMgr.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 2);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        retreivedPlayLaunch = playLaunchEntityMgr.findLatestByPlayId(play.getPid(), states);
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getPid(), playLaunch2.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testUpdateLaunch() throws InterruptedException {
        setupSecurityContext(tenant1);

        playLaunch1 = playLaunchEntityMgr.findByLaunchId(playLaunch1.getLaunchId());
        playLaunch1.setLaunchState(LaunchState.Launched);
        playLaunch1.setAccountsErrored(1L);
        playLaunch1.setAccountsLaunched(5L);
        playLaunch1.setAccountsSuppressed(3L);
        playLaunch1.setContactsLaunched(7L);

        playLaunch2 = playLaunchEntityMgr.findByLaunchId(playLaunch2.getLaunchId());
        playLaunch2.setLaunchState(LaunchState.Launched);
        playLaunch2.setAccountsErrored(2L);
        playLaunch2.setAccountsLaunched(10L);
        playLaunch2.setAccountsSuppressed(5L);
        playLaunch2.setContactsLaunched(8L);

        playLaunchEntityMgr.update(playLaunch1);
        playLaunchEntityMgr.update(playLaunch2);
        Thread.sleep(2000);
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateLaunch" })
    public void testCountDashboard() {
        setupSecurityContext(tenant1);

        Long badPlayId = Long.MAX_VALUE;
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

        checkCountForDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L);

        checkCountForDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L);

        checkCountForDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L);

        checkCountForDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L);

        checkCountForDashboard(null, goodStates, badStates, 0L, 1L, 0L, 2L);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCountDashboard" })
    public void testEntriesDashboard() {
        setupSecurityContext(tenant1);

        Long badPlayId = Long.MAX_VALUE;
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
    public void testCumulativeStatsDashboard() {
        setupSecurityContext(tenant1);

        Long badPlayId = Long.MAX_VALUE;
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

        checkForCumulativeStatsDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L);

        checkForCumulativeStatsDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 8L, 3L,
                15L, 15L, 8L, 3L, 15L, 15L);

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 8L, 3L, 15L, 15L,
                8L, 3L, 15L, 15L);

        checkForCumulativeStatsDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L);

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, 1L, 0L, 0L, 0L, 0L, 8L, 3L, 15L, 15L);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCumulativeStatsDashboard" })
    public void testDelete() {
        setupSecurityContext(tenant1);

        playLaunchEntityMgr.deleteByLaunchId(playLaunch1.getLaunchId());
        System.out.println("deleted " + playLaunch1.getLaunchId());
        playLaunchEntityMgr.deleteByLaunchId(playLaunch2.getLaunchId());
        System.out.println("deleted " + playLaunch2.getLaunchId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        checkNonExistance();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch1 != null && playLaunch1.getLaunchId() != null) {
            playLaunchEntityMgr.deleteByLaunchId(playLaunch1.getLaunchId());
        }
        if (playLaunch2 != null && playLaunch2.getLaunchId() != null) {
            playLaunchEntityMgr.deleteByLaunchId(playLaunch2.getLaunchId());
        }
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
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
        Long dashboardEntriesCount = playLaunchEntityMgr.findDashboardEntriesCount(playId, states, startTimestamp,
                endTimestamp);
        Assert.assertNotNull(dashboardEntriesCount);
        Assert.assertEquals(dashboardEntriesCount.longValue(), expectedCount);
    }

    private void checkForCumulativeStatsDashboard(Long playId, List<LaunchState> goodStates,
            List<LaunchState> badStates, Long startTimestamp, Long endTimestamp, Long suppressed, Long errors,
            Long recommendationsLaunched, Long contactsWithinRecommendations, Long suppressedWithNullEndTimestamp,
            Long errorsWithNullEndTimestamp, Long recommendationsLaunchedWithNullEndTimestamp,
            Long contactsWithinRecommendationsWithNullEndTimestamp) {
        checkForCumulativeStatsDashboard(playId, goodStates, startTimestamp, endTimestamp, suppressed, errors,
                recommendationsLaunched, contactsWithinRecommendations);
        checkForCumulativeStatsDashboard(playId, null, startTimestamp, endTimestamp, suppressed, errors,
                recommendationsLaunched, contactsWithinRecommendations);
        checkForCumulativeStatsDashboard(playId, goodStates, startTimestamp, null, suppressedWithNullEndTimestamp,
                errorsWithNullEndTimestamp, recommendationsLaunchedWithNullEndTimestamp,
                contactsWithinRecommendationsWithNullEndTimestamp);
        checkForCumulativeStatsDashboard(playId, null, startTimestamp, null, suppressedWithNullEndTimestamp,
                errorsWithNullEndTimestamp, recommendationsLaunchedWithNullEndTimestamp,
                contactsWithinRecommendationsWithNullEndTimestamp);
        checkForCumulativeStatsDashboard(playId, badStates, startTimestamp, endTimestamp, 0L, 0L, 0L, 0L);
    }

    private void checkForCumulativeStatsDashboard(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, Long suppressed, Long errors, Long recommendationsLaunched,
            Long contactsWithinRecommendations) {
        Stats cumulativeStats = playLaunchEntityMgr.findDashboardCumulativeStats(playId, states, startTimestamp,
                endTimestamp);
        Assert.assertNotNull(cumulativeStats);
        Assert.assertEquals(cumulativeStats.getSuppressed(), suppressed.longValue());
        Assert.assertEquals(cumulativeStats.getErrors(), errors.longValue());
        Assert.assertEquals(cumulativeStats.getRecommendationsLaunched(), recommendationsLaunched.longValue());
        Assert.assertEquals(cumulativeStats.getContactsWithinRecommendations(),
                contactsWithinRecommendations.longValue());
    }

    private void checkForEntriesDashboard(Long playId, List<LaunchState> goodStates, List<LaunchState> badStates,
            Long startTimestamp, Long offset, Long max, Long endTimestamp, long expectedCount,
            long expectedCountForNullEndTimestmp) {
        checkForEntriesDashboard(playId, goodStates, startTimestamp, offset, max, endTimestamp, expectedCount);
        checkForEntriesDashboard(playId, null, startTimestamp, offset, max, endTimestamp, expectedCount);
        checkForEntriesDashboard(playId, goodStates, startTimestamp, offset, max, null,
                expectedCountForNullEndTimestmp);
        checkForEntriesDashboard(playId, null, startTimestamp, offset, max, null, expectedCountForNullEndTimestmp);
        checkForEntriesDashboard(playId, badStates, startTimestamp, offset, max, endTimestamp, 0);
    }

    private void checkForEntriesDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, Long endTimestamp, long expectedCount) {
        List<LaunchSummary> dashboardEntries = playLaunchEntityMgr.findDashboardEntries(playId, states, startTimestamp,
                offset, max, null, true, endTimestamp);
        Assert.assertNotNull(dashboardEntries);
        Assert.assertEquals(dashboardEntries.size(), expectedCount);

        Set<String> launchIds = new HashSet<>();
        launchIds.add(playLaunch1.getId());
        launchIds.add(playLaunch2.getId());

        if (dashboardEntries.size() > 0) {
            dashboardEntries.stream() //
                    .forEach(entry -> {
                        Assert.assertNotNull(entry.getLaunchId());
                        Assert.assertTrue(launchIds.contains(entry.getLaunchId()));
                        Assert.assertNotNull(entry.getLaunchState());
                        Assert.assertNotNull(entry.getLaunchTime());
                        Assert.assertNotNull(entry.getPlayDisplayName());
                        Assert.assertNotNull(entry.getPlayName());
                        Assert.assertNotNull(entry.getSelectedBuckets());
                    });
        }
    }

    private void checkNonExistance() {
        setupSecurityContext(tenant1);

        PlayLaunch retreivedPlayLaunch = playLaunchEntityMgr.findByLaunchId(playLaunch1.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);
        retreivedPlayLaunch = playLaunchEntityMgr.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNull(retreivedPlayLaunch);
        List<LaunchState> states = new ArrayList<>();
        states.add(LaunchState.Launching);
        List<PlayLaunch> playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);

        playLaunchList = playLaunchEntityMgr.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);
    }
}
