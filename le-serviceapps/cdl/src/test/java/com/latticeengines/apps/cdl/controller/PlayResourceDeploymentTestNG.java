package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.service.impl.TestPlayCreationHelper;
import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;

public class PlayResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private Play play;
    private String playName;
    private PlayLaunch playLaunch;

    private static boolean USE_EXISTING_TENANT = false;
    private static String EXISTING_TENANT = "JLM1526244443808";

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private TestPlayCreationHelper playCreationHelper;

    private RatingEngine ratingEngine;

    private long totalRatedAccounts;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        if (USE_EXISTING_TENANT) {
            setupTestEnvironment(EXISTING_TENANT);
        } else {
            setupTestEnvironment();
            cdlTestDataService.populateData(mainTestTenant.getId());
        }

        playCreationHelper.setTenant(mainTestTenant);
        MetadataSegment retrievedSegment = createSegment();
        ratingEngine = playCreationHelper.createRatingEngine(retrievedSegment, new RatingRule());
    }

    private MetadataSegment createSegment() {
        return playCreationHelper.createSegment(NamingUtils.timestamp("Segment"), null, null);
    }

    @Test(groups = "deployment")
    public void getCrud() {
        playCreationHelper.getCrud();
        playName = playCreationHelper.getPlayName();
    }

    @Test(groups = "deployment", dependsOnMethods = { "getCrud" })
    public void createPlayLaunch() {
        playCreationHelper.createPlayLaunch();
        play = playCreationHelper.getPlay();
        playLaunch = playCreationHelper.getPlayLaunch();
        Assert.assertNotNull(playLaunch.getAccountsSelected());
        Assert.assertNotNull(playLaunch.getAccountsLaunched());
        Assert.assertNotNull(playLaunch.getContactsLaunched());
        Assert.assertNotNull(playLaunch.getAccountsErrored());
        Assert.assertNotNull(playLaunch.getAccountsSuppressed());

        totalRatedAccounts = playLaunch.getAccountsSelected();
    }

    @Test(groups = "deployment", dependsOnMethods = { "createPlayLaunch" })
    private void searchPlayLaunch() {
        List<PlayLaunch> launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName,
                Collections.singletonList(LaunchState.Failed));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        playProxy.updatePlayLaunch(mainTestTenant.getId(), playName, playLaunch.getLaunchId(), LaunchState.Launched);
        playProxy.updatePlayLaunchProgress(mainTestTenant.getId(), playName, playLaunch.getLaunchId(), 100.0D, 8L, 25L,
                0L, (totalRatedAccounts - 8L - 0L));

        launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName,
                Arrays.asList(LaunchState.Canceled, LaunchState.Failed, LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName,
                Collections.singletonList(LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName, null);

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 1);

        launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName,
                Collections.singletonList(LaunchState.Launching));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);

        PlayLaunch retrievedLaunch = playProxy.getPlayLaunch(mainTestTenant.getId(), playName,
                playLaunch.getLaunchId());

        Assert.assertNotNull(retrievedLaunch);
        Assert.assertEquals(retrievedLaunch.getLaunchState(), LaunchState.Launched);
        assertLaunchStats(retrievedLaunch.getAccountsSelected(), totalRatedAccounts);
        assertLaunchStats(retrievedLaunch.getAccountsLaunched(), 8L);
        assertLaunchStats(retrievedLaunch.getContactsLaunched(), 25L);
        assertLaunchStats(retrievedLaunch.getAccountsErrored(), 0L);
        assertLaunchStats(retrievedLaunch.getAccountsSuppressed(), (totalRatedAccounts - 8L - 0L));
    }

    private void assertLaunchStats(Long count, long expectedVal) {
        Assert.assertNotNull(count);
        Assert.assertEquals(count.longValue(), expectedVal);
    }

    @Test(groups = "deployment", dependsOnMethods = { "searchPlayLaunch" })
    private void testGetFullPlays() {
        Play retrievedFullPlay = playProxy.getPlay(mainTestTenant.getId(), playName);
        Assert.assertNotNull(retrievedFullPlay);
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getPlayLaunch());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getMostRecentLaunch());
        // TODO will change to NotNull after integration with RatingEngine is
        // fully done
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewAccountsNum());
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewContactsNum());
        System.out.println("retrievedPlayOverview is " + retrievedFullPlay);

        List<Play> retrievedFullPlayList = playProxy.getPlays(mainTestTenant.getId(), null, null);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetFullPlays" })
    private void testIdempotentCreateOrUpdatePlays() {
        Play createdPlay1 = playProxy.createOrUpdatePlay(mainTestTenant.getId(), play);
        Assert.assertNotNull(createdPlay1.getTalkingPoints());

        List<Play> retrievedFullPlayList = playProxy.getPlays(mainTestTenant.getId(), null, null);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testIdempotentCreateOrUpdatePlays" })
    public void testDeletePlayLaunch() {
        deletePlayLaunch(playName, playLaunch.getLaunchId());

        List<PlayLaunch> launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName,
                Collections.singletonList(LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testDeletePlayLaunch" })
    private void testPlayDelete() {
        List<Play> playList;
        Play retrievedPlay;
        deletePlay(playName);
        retrievedPlay = playProxy.getPlay(mainTestTenant.getId(), playName);
        Assert.assertNull(retrievedPlay);
        playList = playProxy.getPlays(mainTestTenant.getId(), null, null);
        Assert.assertNotNull(playList);
        Assert.assertEquals(playList.size(), 1);
    }

    public void deletePlay(String playName) {
        playProxy.deletePlay(mainTestTenant.getId(), playName, false);
    }

    public void deletePlayLaunch(String playName, String playLaunchId) {
        playProxy.deletePlayLaunch(mainTestTenant.getId(), playName, playLaunchId, false);
    }

    public void useExistingtenant(boolean shouldSkipAutoTenantCreation, Tenant tenant) {
        USE_EXISTING_TENANT = shouldSkipAutoTenantCreation;
        EXISTING_TENANT = tenant.getId();
    }

    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }
}
