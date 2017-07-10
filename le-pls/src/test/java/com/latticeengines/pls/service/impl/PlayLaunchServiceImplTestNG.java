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

    private PlayLaunch playLaunch;

    private String NAME = "play" + CURRENT_TIME_MILLIS;
    private String DISPLAY_NAME = "play Harder";
    private String LAUNCH_DESCRIPTION = "playLaunch done on " + CURRENT_TIME_MILLIS;
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

        playLaunch = new PlayLaunch();
        playLaunch.setDescription(LAUNCH_DESCRIPTION);
        playLaunch.setTenant(tenant1);
        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
    }

    private void cleanupPlayLunches() {
        for (PlayLaunch launch : playLaunchService.findByState(LaunchState.Launching)) {
            playLaunchService.deleteByLaunchId(launch.getLaunchId());
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch != null) {
            playLaunchService.deleteByLaunchId(playLaunch.getLaunchId());
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
    public void testCreateLaunch() {
        playLaunchService.create(playLaunch);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateLaunch" })
    public void testBasicOperations() {
        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch.getLaunchId());
        Assert.assertEquals(retreivedPlayLaunch.getDescription(), LAUNCH_DESCRIPTION);
        Assert.assertNotNull(retreivedPlayLaunch);

        List<LaunchState> states = new ArrayList<>();
        states.add(LaunchState.Launched);
        List<PlayLaunch> playLaunchList = playLaunchService.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);

        states.add(LaunchState.Launching);
        playLaunchList = playLaunchService.findByPlayId(play.getPid(), states);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 1);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        playLaunchList = playLaunchService.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);

        Assert.assertEquals(playLaunchList.size(), 1);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testDelete() {
        setupSecurityContext(tenant1);

        playLaunchService.deleteByLaunchId(playLaunch.getLaunchId());
        System.out.println("deleted " + playLaunch.getLaunchId());
    }

    @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    public void testPostDelete() {
        try {
            Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        PlayLaunch retreivedPlayLaunch = playLaunchService.findByLaunchId(playLaunch.getLaunchId());
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
