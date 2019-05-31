package com.latticeengines.apps.cdl.entitymgr.impl;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayLaunchEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.LaunchSummary;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchDashboard.Stats;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayLaunchEntityMgrImplTestNG extends CDLFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchEntityMgrImplTestNG.class);

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Autowired
    private PlayTypeService playTypeService;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private PlayLaunch playLaunch1;
    private PlayLaunch playLaunch2;
    private PlayLaunch playLaunch_org1_1;
    private PlayLaunch playLaunch_org1_2;
    private PlayLaunch playLaunch_org2_1;
    private PlayLaunch playLaunch_org2_2;

    List<PlayLaunch> allPlayLaunches;
    List<PlayType> types;

    private String org1 = "org1";
    private String destinationAccountIdColumn_1 = "SFDC_ACCOUNT_ID_COL_1";
    private String org2 = "org2";
    private String destinationAccountIdColumn_2 = "SFDC_ACCOUNT_ID_COL_2";

    private CDLExternalSystemType externalSystemType = CDLExternalSystemType.CRM;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String CREATED_BY = "lattice@lattice-engines.com";

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        setupTestEnvironmentWithDummySegment();
        cleanupPlayLunches();

        types = playTypeService.getAllPlayTypes(mainCustomerSpace);
        play = new Play();
        play.setName(NAME);
        play.setTenant(mainTestTenant);
        play.setDisplayName(DISPLAY_NAME);
        play.setPlayType(types.get(0));
        Date timestamp = new Date(System.currentTimeMillis());
        play.setCreated(timestamp);
        play.setUpdated(timestamp);
        play.setCreatedBy(CREATED_BY);
        play.setUpdatedBy(CREATED_BY);
        play.setTargetSegment(testSegment);
        
        playEntityMgr.create(play);
        play = playEntityMgr.getPlayByName(NAME, false);

        playLaunch1 = createPlayLaunch(null, null, null);
        playLaunch2 = createPlayLaunch(null, null, null);

        playLaunch_org1_1 = createPlayLaunch(org1, externalSystemType, destinationAccountIdColumn_1);
        playLaunch_org1_2 = createPlayLaunch(org1, externalSystemType, destinationAccountIdColumn_1);

        playLaunch_org2_1 = createPlayLaunch(org2, externalSystemType, destinationAccountIdColumn_2);
        playLaunch_org2_2 = createPlayLaunch(org2, externalSystemType, destinationAccountIdColumn_2);

        allPlayLaunches = Arrays.asList(playLaunch1, playLaunch2, playLaunch_org1_1, playLaunch_org1_2,
                playLaunch_org2_1, playLaunch_org2_2);
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
        checkNonExistance();
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testCreateLaunch() throws InterruptedException {
        playLaunchEntityMgr.create(playLaunch1);
        Thread.sleep(1000);
        playLaunchEntityMgr.create(playLaunch2);
        long playLaunch1Pid = playLaunch1.getPid();
        long playLaunch2Pid = playLaunch2.getPid();
        Assert.assertTrue(playLaunch1Pid >= PlayLaunch.PID_INIT_VALUE);
        Assert.assertTrue(playLaunch2Pid > playLaunch1Pid);
        Assert.assertNotNull(playLaunch1.getLaunchId());
        Assert.assertNotNull(playLaunch2.getLaunchId());

        Thread.sleep(1000);
        playLaunchEntityMgr.create(playLaunch_org1_1);
        Thread.sleep(1000);
        playLaunchEntityMgr.create(playLaunch_org1_2);
        long playLaunch_org1_1_pid = playLaunch_org1_1.getPid();
        long playLaunch_org1_2_pid = playLaunch_org1_2.getPid();
        Assert.assertTrue(playLaunch_org1_1_pid > playLaunch2Pid);
        Assert.assertTrue(playLaunch_org1_2_pid > playLaunch_org1_1_pid);
        Assert.assertNotNull(playLaunch_org1_1.getLaunchId());
        Assert.assertNotNull(playLaunch_org1_2.getLaunchId());
        Assert.assertEquals(playLaunch_org1_1.getLaunchState(), LaunchState.UnLaunched);
        Assert.assertEquals(playLaunch_org1_1.getLaunchState(), LaunchState.UnLaunched);

        Thread.sleep(1000);
        playLaunchEntityMgr.create(playLaunch_org2_1);
        Thread.sleep(1000);
        playLaunchEntityMgr.create(playLaunch_org2_2);
        Thread.sleep(1000);
        long playLaunch_org2_1_pid = playLaunch_org2_1.getPid();
        long playLaunch_org2_2_pid = playLaunch_org2_2.getPid();
        Assert.assertTrue(playLaunch_org2_1_pid > playLaunch_org1_2_pid);
        Assert.assertTrue(playLaunch_org2_2_pid > playLaunch_org2_1_pid);
        Assert.assertNotNull(playLaunch_org2_1.getLaunchId());
        Assert.assertNotNull(playLaunch_org2_2.getLaunchId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateLaunch" })
    public void testBasicOperations() {

        PlayLaunch retreivedPlayLaunch = playLaunchEntityMgr.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getLaunchId(), playLaunch2.getLaunchId());

        List<LaunchState> states = new ArrayList<>();
        states.add(LaunchState.Launched);
        states.add(LaunchState.Failed);

        List<PlayLaunch> playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);
        playLaunchList.stream().forEach(l -> {
            Assert.assertEquals(l.getPlay().getPid(), play.getPid());
        });

        List<LaunchState> states1 = new ArrayList<>();
        states1.add(LaunchState.UnLaunched);

        playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states1);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 6);
        Assert.assertEquals(playLaunchList.get(4).getPid(), retreivedPlayLaunch.getPid());
        playLaunchList.stream().forEach(l -> {
            Assert.assertEquals(l.getPlay().getPid(), play.getPid());
            Assert.assertTrue(states1.contains(l.getLaunchState()));
        });

        states.add(LaunchState.UnLaunched);

        playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 6);
        Assert.assertEquals(playLaunchList.get(4).getPid(), retreivedPlayLaunch.getPid());
        playLaunchList.stream().forEach(l -> {
            Assert.assertEquals(l.getPlay().getPid(), play.getPid());
            Assert.assertTrue(states1.contains(l.getLaunchState()));
        });

        playLaunchList = playLaunchEntityMgr.findByState(LaunchState.UnLaunched);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 6);
        Assert.assertEquals(playLaunchList.get(4).getPid(), retreivedPlayLaunch.getPid());
        playLaunchList.stream().forEach(l -> {
            Assert.assertEquals(l.getLaunchState(), LaunchState.UnLaunched);
        });

        retreivedPlayLaunch = playLaunchEntityMgr.findLatestByPlayId(play.getPid(), states);
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getPid(), playLaunch_org2_2.getPid());
        Assert.assertTrue(states.contains(retreivedPlayLaunch.getLaunchState()));

        retreivedPlayLaunch = playLaunchEntityMgr.findLatestByPlayAndSysOrg(play.getPid(), org1);
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getPid(), playLaunch_org1_2.getPid());

    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testUpdateLaunch() throws InterruptedException {
        List<LaunchState> states = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.UnLaunched });

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 6, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org2,
                externalSystemType.name());
        playLaunch1 = updatePlayLaunchWithCounts(playLaunch1, LaunchState.Launched, 1L, 5L, 3L, 7L, 1L, 1L);

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 5, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org2,
                externalSystemType.name());
        playLaunch2 = updatePlayLaunchWithCounts(playLaunch2, LaunchState.Launched, 2L, 10L, 5L, 8L, 2L, 2L);

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 4, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org2,
                externalSystemType.name());
        playLaunch_org1_1 = updatePlayLaunchWithCounts(playLaunch_org1_1, LaunchState.Launched, 0L, 12L, 3L, 0L, 0L,
                0L);

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 3, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 1, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org2,
                externalSystemType.name());
        playLaunch_org1_2 = updatePlayLaunchWithCounts(playLaunch_org1_2, LaunchState.Launched, 3L, 8L, 6L, 10L, 2L,
                2L);

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 0, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 2, org2,
                externalSystemType.name());
        playLaunch_org2_1 = updatePlayLaunchWithCounts(playLaunch_org2_1, LaunchState.Launched, 2L, 17L, 2L, 4L, 1L,
                1L);

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 1, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 0, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 1, org2,
                externalSystemType.name());
        playLaunch_org2_2 = updatePlayLaunchWithCounts(playLaunch_org2_2, LaunchState.Launched, 5L, 4L, 0L, 18L, 2L,
                2L);

        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 0, null, null);
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 0, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), states, 0L, System.currentTimeMillis(), 0, org2,
                externalSystemType.name());
    }

    @Test(groups = "functional", dependsOnMethods = { "testUpdateLaunch" })
    public void testCountDashboard() {

        Long badPlayId = Long.MAX_VALUE;
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

        checkCountForDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, null, null);
        checkCountForDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, org1,
                externalSystemType.name());
        checkCountForDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, org2,
                externalSystemType.name());

        checkCountForDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 6L, 6L, null,
                null);
        checkCountForDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L, org1,
                externalSystemType.name());
        checkCountForDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L, org2,
                externalSystemType.name());

        checkCountForDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 6L, 6L, null, null);
        checkCountForDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L, org1,
                externalSystemType.name());
        checkCountForDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 2L, org2,
                externalSystemType.name());

        checkCountForDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, null, null);
        checkCountForDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, org1, externalSystemType.name());
        checkCountForDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, org1, externalSystemType.name());

        checkCountForDashboard(null, goodStates, badStates, 0L, 1L, 0L, 6L, null, null);
        checkCountForDashboard(null, goodStates, badStates, 0L, 1L, 0L, 2L, org1, externalSystemType.name());
        checkCountForDashboard(null, goodStates, badStates, 0L, 1L, 0L, 2L, org2, externalSystemType.name());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCountDashboard" })
    public void testEntriesDashboard() {
        Long badPlayId = Long.MAX_VALUE;
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

        checkForEntriesDashboard(badPlayId, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 0L, 0L,
                null, null);

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 6L, 6L,
                null, null);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 6L, 6L, null,
                null);

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L,
                null, null);

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L,
                null, null);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L, null,
                null);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L, null,
                null);

        checkForEntriesDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1), 0L, 10L,
                System.currentTimeMillis(), 0L, 0L, null, null);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, 1L, 0L, 6L, null, null);

        //

        checkForEntriesDashboard(badPlayId, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 0L, 0L,
                org1, externalSystemType.name());

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 2L, 2L,
                org1, externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 2L, 2L, org1,
                externalSystemType.name());

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L,
                org1, externalSystemType.name());

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L,
                org1, externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L, org1,
                externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L, org1,
                externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1), 0L, 10L,
                System.currentTimeMillis(), 0L, 0L, org1, externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, 1L, 0L, 2L, org1, externalSystemType.name());

        checkForEntriesDashboard(badPlayId, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 0L, 0L,
                org2, externalSystemType.name());

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 2L, 2L,
                org2, externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, System.currentTimeMillis(), 2L, 2L, org2,
                externalSystemType.name());

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L,
                org2, externalSystemType.name());

        checkForEntriesDashboard(play.getPid(), goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L,
                org2, externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 1L, System.currentTimeMillis(), 1L, 1L, org2,
                externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 1L, 1L, System.currentTimeMillis(), 1L, 1L, org2,
                externalSystemType.name());

        checkForEntriesDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1), 0L, 10L,
                System.currentTimeMillis(), 0L, 0L, null, null);

        checkForEntriesDashboard(null, goodStates, badStates, 0L, 0L, 10L, 1L, 0L, 2L, org2, externalSystemType.name());

    }

    @Test(groups = "functional", dependsOnMethods = { "testEntriesDashboard" })
    public void testCumulativeStatsDashboard() throws Exception {

        Long badPlayId = Long.MAX_VALUE;
        List<LaunchState> goodStates = Arrays.asList(new LaunchState[] { LaunchState.Launched, LaunchState.Launching });
        List<LaunchState> badStates = Arrays.asList(new LaunchState[] { LaunchState.Failed, LaunchState.Launching });

        checkForCumulativeStatsDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null, null);

        checkForCumulativeStatsDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 19L, 13L,
                56L, 47L, 19L, 13L, 56L, 47L, 8L, 8L, 8L, 8L, null, null);

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 19L, 13L, 56L,
                47L, 19L, 13L, 56L, 47L, 8L, 8L, 8L, 8L, null, null);

        checkForCumulativeStatsDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, null, null);

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, 1L, 0L, 0L, 0L, 0L, 19L, 13L, 56L, 47L, 0L,
                0L, 8L, 8L, null, null);

        //

        checkForCumulativeStatsDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, org1, externalSystemType.name());

        checkForCumulativeStatsDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 9L, 3L,
                20L, 10L, 9L, 3L, 20L, 10L, 2L, 2L, 2L, 2L, org1, externalSystemType.name());

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 9L, 3L, 20L, 10L,
                9L, 3L, 20L, 10L, 2L, 2L, 2L, 2L, org1, externalSystemType.name());

        checkForCumulativeStatsDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, org1,
                externalSystemType.name());

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, 1L, 0L, 0L, 0L, 0L, 9L, 3L, 20L, 10L, 0L, 0L,
                2L, 2L,
                org1, externalSystemType.name());

        //

        checkForCumulativeStatsDashboard(badPlayId, goodStates, badStates, 0L, System.currentTimeMillis(), 0L, 0L, 0L,
                0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, org2, externalSystemType.name());

        checkForCumulativeStatsDashboard(play.getPid(), goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 7L,
                21L, 22L, 2L, 7L, 21L, 22L, 3L, 3L, 3L, 3L, org2, externalSystemType.name());

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, System.currentTimeMillis(), 2L, 7L, 21L, 22L,
                2L, 7L, 21L, 22L, 3L, 3L, 3L, 3L, org2, externalSystemType.name());

        checkForCumulativeStatsDashboard(null, goodStates, badStates, (System.currentTimeMillis() - 1),
                System.currentTimeMillis(), 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, org2,
                externalSystemType.name());

        checkForCumulativeStatsDashboard(null, goodStates, badStates, 0L, 1L, 0L, 0L, 0L, 0L, 2L, 7L, 21L, 22L, 0L, 0L,
                3L, 3L,
                org2, externalSystemType.name());

        throw new Exception();

    }

    @Test(groups = "functional", dependsOnMethods = { "testCumulativeStatsDashboard" })
    public void testDelete() {
        playLaunchEntityMgr.deleteByLaunchId(playLaunch1.getLaunchId(), false);
        playLaunchEntityMgr.deleteByLaunchId(playLaunch2.getLaunchId(), false);
        playLaunchEntityMgr.deleteByLaunchId(playLaunch_org1_1.getLaunchId(), false);
        playLaunchEntityMgr.deleteByLaunchId(playLaunch_org1_2.getLaunchId(), false);
        playLaunchEntityMgr.deleteByLaunchId(playLaunch_org2_1.getLaunchId(), false);
        playLaunchEntityMgr.deleteByLaunchId(playLaunch_org2_2.getLaunchId(), false);
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(10L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        checkNonExistance();
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch1 != null && playLaunch1.getLaunchId() != null) {
            playLaunchEntityMgr.deleteByLaunchId(playLaunch1.getLaunchId(), false);
        }
        if (playLaunch2 != null && playLaunch2.getLaunchId() != null) {
            playLaunchEntityMgr.deleteByLaunchId(playLaunch2.getLaunchId(), false);
        }
        Tenant tenant1 = tenantService.findByTenantId("testTenant1");
        if (tenant1 != null) {
            tenantService.discardTenant(tenant1);
        }
    }

    private PlayLaunch createPlayLaunch(String org, CDLExternalSystemType externalSystemType,
                                        String destinationAccountIdColumn) {
        PlayLaunch launch = new PlayLaunch();
        launch.setTenant(mainTestTenant);
        launch.setLaunchState(LaunchState.UnLaunched);
        launch.setPlay(play);
        launch.setDestinationOrgId(org);
        launch.setDestinationSysType(externalSystemType);
        launch.setDestinationAccountId(destinationAccountIdColumn);
        launch.setCreatedBy(CREATED_BY);
        launch.setUpdatedBy(CREATED_BY);
        launch.setLaunchId(NamingUtils.randomSuffix("pl", 16));
        return launch;
    }

    private void cleanupPlayLunches() {
        for (PlayLaunch launch : playLaunchEntityMgr.findByState(LaunchState.UnLaunched)) {
            playLaunchEntityMgr.deleteByLaunchId(launch.getLaunchId(), false);
        }
    }

    private PlayLaunch updatePlayLaunchWithCounts(PlayLaunch launch, LaunchState state, long errored,
            long accountsLaunched, long suppressed, long contactsLaunched, long contactsSuppressed,
            long contactsErrored) throws InterruptedException {
        launch = playLaunchEntityMgr.findByLaunchId(launch.getLaunchId());
        launch.setLaunchState(state);
        launch.setAccountsErrored(errored);
        launch.setAccountsLaunched(accountsLaunched);
        launch.setAccountsSuppressed(suppressed);
        launch.setContactsLaunched(contactsLaunched);
        launch.setContactsSuppressed(contactsSuppressed);
        launch.setContactsErrored(contactsErrored);
        playLaunchEntityMgr.update(launch);
        Thread.sleep(1000);
        return launch;
    }

    private void checkCountForDashboard(Long playId, List<LaunchState> goodStates, List<LaunchState> badStates,
            Long startTimestamp, Long endTimestamp, long expectedCount, long expectedCountForNullEndTimestmp,
            String orgId, String destinationSystemType) {
        checkCountForDashboard(playId, goodStates, startTimestamp, endTimestamp, expectedCount, orgId,
                destinationSystemType);
        checkCountForDashboard(playId, null, startTimestamp, endTimestamp, expectedCount, orgId, destinationSystemType);
        checkCountForDashboard(playId, goodStates, startTimestamp, null, expectedCountForNullEndTimestmp, orgId,
                destinationSystemType);
        checkCountForDashboard(playId, null, startTimestamp, null, expectedCountForNullEndTimestmp, orgId,
                destinationSystemType);
        checkCountForDashboard(playId, badStates, startTimestamp, endTimestamp, 0, orgId, destinationSystemType);
    }

    private void checkCountForDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long endTimestamp,
            long expectedCount, String orgId, String destinationSystemType) {
        Long dashboardEntriesCount = playLaunchEntityMgr.findDashboardEntriesCount(playId, states, startTimestamp,
                endTimestamp, orgId, destinationSystemType);
        Assert.assertNotNull(dashboardEntriesCount);
        Assert.assertEquals(dashboardEntriesCount.longValue(), expectedCount);
    }

    private void checkForCumulativeStatsDashboard(Long playId, List<LaunchState> goodStates,
            List<LaunchState> badStates, Long startTimestamp, Long endTimestamp, Long accountsSuppressed,
            Long accountsErrors, Long recommendationsLaunched, Long contactsWithinRecommendations,
            Long suppressedWithNullEndTimestamp, Long errorsWithNullEndTimestamp,
            Long recommendationsLaunchedWithNullEndTimestamp, Long contactsWithinRecommendationsWithNullEndTimestamp,
            Long contactsSuppressed, Long contactErrors, Long contactsSuppressedWithNullEndTimestamp,
            Long contactErrorsWithNullEndTimestamp, String orgId, String destinationSystemType) {
        checkForCumulativeStatsDashboard(playId, goodStates, startTimestamp, endTimestamp, accountsSuppressed,
                accountsErrors,
                recommendationsLaunched, contactsWithinRecommendations, contactsSuppressed, contactErrors, orgId,
                destinationSystemType);
        checkForCumulativeStatsDashboard(playId, null, startTimestamp, endTimestamp, accountsSuppressed, accountsErrors,
                recommendationsLaunched, contactsWithinRecommendations, contactsSuppressed, contactErrors, orgId,
                destinationSystemType);
        checkForCumulativeStatsDashboard(playId, goodStates, startTimestamp, null, suppressedWithNullEndTimestamp,
                errorsWithNullEndTimestamp, recommendationsLaunchedWithNullEndTimestamp,
                contactsWithinRecommendationsWithNullEndTimestamp, contactsSuppressedWithNullEndTimestamp,
                contactErrorsWithNullEndTimestamp, orgId,
                destinationSystemType);
        checkForCumulativeStatsDashboard(playId, null, startTimestamp, null, suppressedWithNullEndTimestamp,
                errorsWithNullEndTimestamp, recommendationsLaunchedWithNullEndTimestamp,
                contactsWithinRecommendationsWithNullEndTimestamp, contactsSuppressedWithNullEndTimestamp,
                contactErrorsWithNullEndTimestamp, orgId,
                destinationSystemType);
        checkForCumulativeStatsDashboard(playId, badStates, startTimestamp, endTimestamp, 0L, 0L, 0L, 0L, 0L, 0L, orgId,
                destinationSystemType);
    }

    private void checkForCumulativeStatsDashboard(Long playId, List<LaunchState> states, Long startTimestamp,
            Long endTimestamp, Long accountsSuppressed, Long accountErrors, Long recommendationsLaunched,
            Long contactsWithinRecommendations, Long contactsSuppressed, Long contactsErrored, String orgId,
            String destinationSystemType) {
        Stats cumulativeStats = playLaunchEntityMgr.findDashboardCumulativeStats(playId, states, startTimestamp,
                endTimestamp, orgId, destinationSystemType);
        Assert.assertNotNull(cumulativeStats);
        Assert.assertEquals(cumulativeStats.getAccountsSuppressed(), accountsSuppressed.longValue());
        Assert.assertEquals(cumulativeStats.getAccountErrors(), accountErrors.longValue());
        Assert.assertEquals(cumulativeStats.getRecommendationsLaunched(), recommendationsLaunched.longValue());
        Assert.assertEquals(cumulativeStats.getContactsWithinRecommendations(),
                contactsWithinRecommendations.longValue());
        Assert.assertEquals(cumulativeStats.getContactsSuppressed(), contactsSuppressed.longValue());
        Assert.assertEquals(cumulativeStats.getContactErrors(), contactsErrored.longValue());

        List<Play> uniquePlays = playLaunchEntityMgr.findDashboardPlaysWithLaunches(playId, states, startTimestamp,
                endTimestamp, null, null);
        Assert.assertNotNull(uniquePlays);

        if (recommendationsLaunched > 0L) {
            Set<String> playIdSet = ConcurrentHashMap.newKeySet();

            Assert.assertTrue(uniquePlays.size() > 0);
            uniquePlays.stream().forEach(pl -> {
                Assert.assertNotNull(pl.getPid());
                Assert.assertNotNull(pl.getName());
                Assert.assertNotNull(pl.getDisplayName());
                Assert.assertFalse(playIdSet.contains(pl.getName()));
                playIdSet.add(pl.getName());
            });
        }

        List<Pair<String, String>> dashboardEntries = playLaunchEntityMgr.findDashboardOrgIdWithLaunches(playId, states,
                startTimestamp, endTimestamp, orgId, destinationSystemType);
        Assert.assertNotNull(dashboardEntries);
        if (recommendationsLaunched > 0) {
            Assert.assertTrue(dashboardEntries.size() > 0);
            Set<String> orgSet = new HashSet<>(Arrays.asList(org1, org2));
            dashboardEntries.stream() //
                    .forEach(pair -> {
                        Assert.assertNotNull(pair.getLeft());
                        Assert.assertNotNull(pair.getRight());
                        Assert.assertTrue(orgSet.contains(pair.getLeft()));
                        Assert.assertEquals(CDLExternalSystemType.valueOf(pair.getRight()), CDLExternalSystemType.CRM);
                    });
        }
    }

    private void checkForEntriesDashboard(Long playId, List<LaunchState> goodStates, List<LaunchState> badStates,
            Long startTimestamp, Long offset, Long max, Long endTimestamp, long expectedCount,
            long expectedCountForNullEndTimestmp, String orgId, String destinationSystemType) {
        checkForEntriesDashboard(playId, goodStates, startTimestamp, offset, max, endTimestamp, expectedCount, orgId,
                destinationSystemType);
        checkForEntriesDashboard(playId, null, startTimestamp, offset, max, endTimestamp, expectedCount, orgId,
                destinationSystemType);
        checkForEntriesDashboard(playId, goodStates, startTimestamp, offset, max, null, expectedCountForNullEndTimestmp,
                orgId, destinationSystemType);
        checkForEntriesDashboard(playId, null, startTimestamp, offset, max, null, expectedCountForNullEndTimestmp,
                orgId, destinationSystemType);
        checkForEntriesDashboard(playId, badStates, startTimestamp, offset, max, endTimestamp, 0, orgId,
                destinationSystemType);
    }

    private void checkForEntriesDashboard(Long playId, List<LaunchState> states, Long startTimestamp, Long offset,
            Long max, Long endTimestamp, long expectedCount, String orgId, String destinationSystemType) {
        checkForEntriesDashboardWithSortOrder(playId, states, startTimestamp, offset, max, endTimestamp, "created",
                true, expectedCount, orgId, destinationSystemType);
        checkForEntriesDashboardWithSortOrder(playId, states, startTimestamp, offset, max, endTimestamp, "created",
                false, expectedCount, orgId, destinationSystemType);
        checkForEntriesDashboardWithSortOrder(playId, states, startTimestamp, offset, max, endTimestamp, null, true,
                expectedCount, orgId, destinationSystemType);
        checkForEntriesDashboardWithSortOrder(playId, states, startTimestamp, offset, max, endTimestamp, null, false,
                expectedCount, orgId, destinationSystemType);
        checkForEntriesDashboardWithSortOrder(playId, states, startTimestamp, offset, max, endTimestamp, "launchState",
                true, expectedCount, orgId, destinationSystemType);
        checkForEntriesDashboardWithSortOrder(playId, states, startTimestamp, offset, max, endTimestamp, "launchState",
                false, expectedCount, orgId, destinationSystemType);
    }

    private void checkForEntriesDashboardWithSortOrder(Long playId, List<LaunchState> states, Long startTimestamp,
            Long offset, Long max, Long endTimestamp, String sortBy, boolean descending, long expectedCount,
            String orgId, String destinationSystemType) {
        List<LaunchSummary> dashboardEntries = playLaunchEntityMgr.findDashboardEntries(playId, states, startTimestamp,
                offset, max, sortBy, descending, endTimestamp, orgId, destinationSystemType);
        Assert.assertNotNull(dashboardEntries);
        Assert.assertEquals(dashboardEntries.size(), expectedCount);

        Set<String> launchIds = new HashSet<>();
        launchIds.add(playLaunch1.getId());
        launchIds.add(playLaunch2.getId());
        launchIds.add(playLaunch_org1_1.getId());
        launchIds.add(playLaunch_org1_2.getId());
        launchIds.add(playLaunch_org2_1.getId());
        launchIds.add(playLaunch_org2_2.getId());

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

            if (descending) {
                if (dashboardEntries.size() > 0) {
                    if (orgId == null) {
                        Assert.assertEquals(dashboardEntries.get(0).getLaunchId(),
                                offset == 0 ? playLaunch_org2_2.getId() : playLaunch_org2_1.getId());
                    } else if (org1.equals(orgId)) {
                        Assert.assertEquals(dashboardEntries.get(0).getLaunchId(),
                                offset == 0 ? playLaunch_org1_2.getId() : playLaunch_org1_1.getId());
                    } else if (org2.equals(orgId)) {
                        Assert.assertEquals(dashboardEntries.get(0).getLaunchId(),
                                offset == 0 ? playLaunch_org2_2.getId() : playLaunch_org2_1.getId());
                    }
                }
                if (orgId == null) {
                    if (dashboardEntries.size() >= 6) {
                        Assert.assertEquals(dashboardEntries.get(5).getLaunchId(), playLaunch1.getId());
                    }
                } else if (org1.equals(orgId)) {
                    if (dashboardEntries.size() >= 2) {
                        Assert.assertEquals(dashboardEntries.get(1).getLaunchId(), playLaunch_org1_1.getId());
                    }
                } else if (org2.equals(orgId)) {
                    if (dashboardEntries.size() >= 2) {
                        Assert.assertEquals(dashboardEntries.get(1).getLaunchId(), playLaunch_org2_1.getId());
                    }
                }
            }
        } else {
            if (dashboardEntries.size() > 0) {
                if (orgId == null) {
                    Assert.assertEquals(dashboardEntries.get(0).getLaunchId(),
                            offset == 0 ? playLaunch1.getId() : playLaunch2.getId());
                } else if (org1.equals(orgId)) {
                    Assert.assertEquals(dashboardEntries.get(0).getLaunchId(),
                            offset == 0 ? playLaunch_org1_1.getId() : playLaunch_org1_2.getId());
                } else if (org2.equals(orgId)) {
                    Assert.assertEquals(dashboardEntries.get(0).getLaunchId(),
                            offset == 0 ? playLaunch_org2_1.getId() : playLaunch_org2_2.getId());
                }
            }
            if (orgId == null) {
                if (dashboardEntries.size() >= 6) {
                    Assert.assertEquals(dashboardEntries.get(5).getLaunchId(), playLaunch_org2_2.getId());
                }
            } else if (org1.equals(orgId)) {
                if (dashboardEntries.size() >= 2) {
                    Assert.assertEquals(dashboardEntries.get(1).getLaunchId(), playLaunch_org1_2.getId());
                }
            } else if (org2.equals(orgId)) {
                if (dashboardEntries.size() >= 2) {
                    Assert.assertEquals(dashboardEntries.get(1).getLaunchId(), playLaunch_org2_2.getId());
                }
            }
        }
    }

    private void checkNonExistance() {

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
