package com.latticeengines.pls.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
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
    private String LAUNCH_DESCRIPTION_1 = "playLaunch1 done on " + CURRENT_TIME_MILLIS;
    private String LAUNCH_DESCRIPTION_2 = "playLaunch2 done on " + CURRENT_TIME_MILLIS;
    private String CREATED_BY = "lattice@lattice-engines.com";

    private Tenant tenant1;

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
        play.setTimestamp(timestamp);
        play.setLastUpdatedTimestamp(timestamp);
        play.setCreatedBy(CREATED_BY);

        playEntityMgr.create(play);
        play = playEntityMgr.findByName(NAME);

        playLaunch1 = new PlayLaunch();
        playLaunch1.setDescription(LAUNCH_DESCRIPTION_1);
        playLaunch1.setTenant(tenant1);
        playLaunch1.setLaunchState(LaunchState.Launching);
        playLaunch1.setPlay(play);

        playLaunch2 = new PlayLaunch();
        playLaunch2.setDescription(LAUNCH_DESCRIPTION_2);
        playLaunch2.setTenant(tenant1);
        playLaunch2.setLaunchState(LaunchState.Launching);
        playLaunch2.setPlay(play);
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
        Thread.sleep(500);
        playLaunchService.create(playLaunch2);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateLaunch" })
    public void testBasicOperations() {
        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch2.getLaunchId());
        Assert.assertNotNull(retreivedPlayLaunch);
        Assert.assertEquals(retreivedPlayLaunch.getDescription(), LAUNCH_DESCRIPTION_2);

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
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
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
