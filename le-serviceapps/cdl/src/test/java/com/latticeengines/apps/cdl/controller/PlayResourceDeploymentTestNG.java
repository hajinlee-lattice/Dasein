package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLDeploymentTestNGBase;
import com.latticeengines.common.exposed.util.NamingUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.MetadataSegment;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayLaunchChannel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class PlayResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private Play play;
    private String playName;
    PlayLaunch playLaunch;

    private static boolean USE_EXISTING_TENANT = false;
    private static String EXISTING_TENANT = "JLM1526244443808";

    @Value("${common.test.pls.url}")
    private String internalResourceHostPort;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private TestPlayCreationHelper playCreationHelper;

    private RatingEngine ratingEngine;

    private long totalRatedAccounts;

    PlayLaunchConfig playLaunchConfig = null;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        if (USE_EXISTING_TENANT) {
            setupTestEnvironment(EXISTING_TENANT);
        } else {
            setupTestEnvironment();
            cdlTestDataService.populateData(mainTestTenant.getId(), 3);
        }

        playLaunchConfig = new PlayLaunchConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                .destinationSystemName(CDLExternalSystemName.Salesforce)
                .destinationSystemId("Salesforce_" + System.currentTimeMillis())
                .trayAuthenticationId(UUID.randomUUID().toString()).audienceId(UUID.randomUUID().toString()).build();

        playCreationHelper.setTenant(mainTestTenant);
        playCreationHelper.setDestinationOrgId(playLaunchConfig.getDestinationSystemId());
        playCreationHelper.setDestinationOrgType(playLaunchConfig.getDestinationSystemType());
        MetadataSegment retrievedSegment = createSegment();
        playCreationHelper.createPlayTargetSegment();
        playCreationHelper.createLookupIdMapping(playLaunchConfig);
        ratingEngine = playCreationHelper.createRatingEngine(retrievedSegment, new RatingRule());
    }

    private MetadataSegment createSegment() {
        return playCreationHelper.createSegment(NamingUtils.timestamp("Segment"), null, null);
    }

    @Test(groups = "deployment-app")
    public void testCrud() {
        playCreationHelper.createDefaultPlayAndTestCrud(playLaunchConfig);
        playName = playCreationHelper.getPlayName();
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCrud" })
    public void createPlayLaunch() {
        playCreationHelper.createPlayLaunch(playLaunchConfig);
        play = playCreationHelper.getPlay();
        playLaunch = playCreationHelper.getPlayLaunch();
        playLaunch = playProxy.launchPlay(mainTestTenant.getId(), play.getName(), playLaunch.getLaunchId(), true);
        Assert.assertNotNull(playLaunch.getAccountsSelected());
        Assert.assertNotNull(playLaunch.getAccountsLaunched());
        Assert.assertNotNull(playLaunch.getContactsLaunched());
        Assert.assertNotNull(playLaunch.getAccountsErrored());
        Assert.assertNotNull(playLaunch.getAccountsSuppressed());

        totalRatedAccounts = playLaunch.getAccountsSelected();
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "createPlayLaunch" })
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

        List<PlayLaunchChannel> configurations = playProxy.getPlayLaunchChannels(mainTestTenant.getId(), playName,
                true);
        Assert.assertNotNull(configurations);
        // Assert.assertEquals(configurations.getLaunchChannelMap().get(playLaunch.getDestinationOrgId()).getPid(),
        // playLaunch.getPid());

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

    @Test(groups = "deployment-app", dependsOnMethods = { "searchPlayLaunch" })
    public void testGetFullPlays() {
        Play retrievedFullPlay = playProxy.getPlay(mainTestTenant.getId(), playName);
        Assert.assertNotNull(retrievedFullPlay);
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory());
        Assert.assertNull(retrievedFullPlay.getLaunchHistory().getLastIncompleteLaunch());
        Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getLastCompletedLaunch());
        // TODO will change to NotNull after integration with RatingEngine is
        // fully done
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewAccountsNum());
        // Assert.assertNotNull(retrievedFullPlay.getLaunchHistory().getNewContactsNum());
        System.out.println("retrievedPlayOverview is " + retrievedFullPlay);

        List<Play> retrievedFullPlayList = playProxy.getPlays(mainTestTenant.getId(), null, null);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testGetFullPlays" })
    public void testIdempotentCreateOrUpdatePlays() {
        Play createdPlay1 = playProxy.createOrUpdatePlay(mainTestTenant.getId(), play);
        Assert.assertNotNull(createdPlay1.getTalkingPoints());

        List<Play> retrievedFullPlayList = playProxy.getPlays(mainTestTenant.getId(), null, null);
        Assert.assertNotNull(retrievedFullPlayList);
        Assert.assertEquals(retrievedFullPlayList.size(), 2);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testIdempotentCreateOrUpdatePlays" })
    public void testDeletePlayLaunch() {
        deletePlayLaunch(playName, playLaunch.getLaunchId());

        List<PlayLaunch> launchList = playProxy.getPlayLaunches(mainTestTenant.getId(), playName,
                Collections.singletonList(LaunchState.Launched));

        Assert.assertNotNull(launchList);
        Assert.assertEquals(launchList.size(), 0);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testDeletePlayLaunch" })
    public void testPlayDelete() {
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
