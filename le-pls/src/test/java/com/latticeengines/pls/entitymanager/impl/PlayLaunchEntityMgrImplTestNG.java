package com.latticeengines.pls.entitymanager.impl;

import java.util.List;

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
import com.latticeengines.pls.entitymanager.PlayLaunchEntityMgr;
import com.latticeengines.pls.functionalframework.PlsFunctionalTestNGBase;
import com.latticeengines.security.exposed.service.TenantService;

public class PlayLaunchEntityMgrImplTestNG extends PlsFunctionalTestNGBase {

    private static long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    @Autowired
    private PlayEntityMgr playEntityMgr;

    @Autowired
    private PlayLaunchEntityMgr playLaunchEntityMgr;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private PlayLaunch playLaunch;

    private final static String NAME = "play" + CURRENT_TIME_MILLIS;
    private final static String DISPLAY_NAME = "play Harder";
    private final static String LAUNCH_NAME = "playLaunch" + CURRENT_TIME_MILLIS;

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

        playEntityMgr.create(play);
        play = playEntityMgr.findByName(NAME);

        playLaunch = new PlayLaunch();
        playLaunch.setName(LAUNCH_NAME);
        playLaunch.setTenant(tenant1);
        playLaunch.setLaunchState(LaunchState.Launching);
        playLaunch.setPlay(play);
    }

    private void cleanupPlayLunches() {
        for (PlayLaunch launch : playLaunchEntityMgr.findByState(LaunchState.Launching)) {
            playLaunchEntityMgr.deleteByName(launch.getName());
        }
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
        if (playLaunch != null) {
            playLaunchEntityMgr.deleteByName(LAUNCH_NAME);
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
        playLaunchEntityMgr.create(playLaunch);
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateLaunch" })
    public void testBasicOperations() {
        PlayLaunch retreivedPlayLaunch = playLaunchEntityMgr.findByName(LAUNCH_NAME);
        Assert.assertEquals(retreivedPlayLaunch.getName(), LAUNCH_NAME);
        Assert.assertNotNull(retreivedPlayLaunch);

        List<PlayLaunch> playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 1);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());

        playLaunchList = playLaunchEntityMgr.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);

        Assert.assertEquals(playLaunchList.size(), 1);
        Assert.assertEquals(playLaunchList.get(0).getPid(), retreivedPlayLaunch.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testBasicOperations" })
    public void testDelete() {
        setupSecurityContext(tenant1);

        playLaunchEntityMgr.deleteByName(LAUNCH_NAME);
        System.out.println("deleted " + LAUNCH_NAME);
    }

    // TODO - anoop - enable it after fixing issue in delete
    //
    // @Test(groups = "functional", dependsOnMethods = { "testDelete" })
    // public void testPostDelete() {
    // try {
    // Thread.sleep(TimeUnit.SECONDS.toMillis(5L));
    // } catch (InterruptedException e) {
    // // TODO Auto-generated catch block
    // e.printStackTrace();
    // }
    // checkNonExistance();
    // }

    private void checkNonExistance() {
        setupSecurityContext(tenant1);

        PlayLaunch retreivedPlayLaunch = playLaunchEntityMgr.findByName(LAUNCH_NAME);
        Assert.assertNull(retreivedPlayLaunch);

        List<PlayLaunch> playLaunchList = playLaunchEntityMgr.findByPlayId(play.getPid(), LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);

        playLaunchList = playLaunchEntityMgr.findByState(LaunchState.Launching);
        Assert.assertNotNull(playLaunchList);
        Assert.assertEquals(playLaunchList.size(), 0);
    }
}
