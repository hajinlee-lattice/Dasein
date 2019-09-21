package com.latticeengines.apps.cdl.controller;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

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
import com.latticeengines.domain.exposed.pls.RatingBucketName;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingRule;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.exposed.service.CDLTestDataService;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class PlayResourceDeploymentTestNG extends CDLDeploymentTestNGBase {

    private Play play;
    private String playName;
    PlayLaunch playLaunch;

    @Value("${common.test.pls.url}")
    private String internalResourceHostPort;

    @Value("${cdl.play.service.default.types.user}")
    private String serviceUser;

    @Inject
    private CDLTestDataService cdlTestDataService;

    @Inject
    private PlayProxy playProxy;

    @Inject
    private TestPlayCreationHelper playCreationHelper;

    private RatingEngine ratingEngine;

    private long totalRatedAccounts;

    TestPlaySetupConfig testPlaySetupConfig = null;

    @BeforeClass(groups = "deployment-app")
    public void setup() throws Exception {
        String existingTenant = null;
        testPlaySetupConfig = new TestPlaySetupConfig.Builder().existingTenant(existingTenant)
                .addChannel(new TestPlayChannelConfig.Builder().destinationSystemType(CDLExternalSystemType.CRM)
                        .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.A, RatingBucketName.B)))
                        .destinationSystemName(CDLExternalSystemName.Salesforce)
                        .destinationSystemId(CDLExternalSystemName.Salesforce.name() + System.currentTimeMillis())
                        .isAlwaysOn(true).cronSchedule("0 0 12 ? * THU *").build())
                .build();

        playCreationHelper.setupTenantAndData(testPlaySetupConfig);
        mainTestTenant = playCreationHelper.getTenant();

        MetadataSegment retrievedSegment = playCreationHelper.createSegment(NamingUtils.timestamp("Segment"), null,
                null);
        ratingEngine = playCreationHelper.createRatingEngine(retrievedSegment, new RatingRule());

        playCreationHelper.createPlayTargetSegment();
        playCreationHelper.createLookupIdMapping(testPlaySetupConfig);
    }

    @Test(groups = "deployment-app")
    public void testCrud() {
        playCreationHelper.createDefaultPlayAndTestCrud(testPlaySetupConfig);
        playName = playCreationHelper.getPlayName();
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCrud" })
    public void createPlayLaunchFromChannel() {
        String testuser = "testuser@lattice-engines.com";
        play = playCreationHelper.getPlay();
        PlayLaunchChannel channel = playProxy
                .getPlayLaunchChannels(playCreationHelper.getCustomerSpace(), play.getName(), false).get(0);
        channel.setUpdatedBy(testuser);
        playProxy.updatePlayLaunchChannel(playCreationHelper.getCustomerSpace(), play.getName(), channel.getId(),
                channel, false);

        // Mimicking a manual Launch creation
        PlayLaunch testPlayLaunch = playProxy.queueNewLaunchByPlayAndChannel(playCreationHelper.getCustomerSpace(),
                play.getName(), channel.getId());
        Assert.assertNotNull(testPlayLaunch.getAccountsSelected());
        Assert.assertNotNull(testPlayLaunch.getAccountsLaunched());
        Assert.assertNotNull(testPlayLaunch.getContactsLaunched());
        Assert.assertNotNull(testPlayLaunch.getAccountsErrored());
        Assert.assertNotNull(testPlayLaunch.getAccountsSuppressed());
        Assert.assertEquals(testPlayLaunch.getCreatedBy(), testuser);
        Assert.assertEquals(testPlayLaunch.getUpdatedBy(), testuser);
        Assert.assertEquals(testPlayLaunch.getLaunchState(), LaunchState.Queued);
        totalRatedAccounts = testPlayLaunch.getAccountsSelected();

        playProxy.deletePlayLaunch(playCreationHelper.getCustomerSpace(), playCreationHelper.getPlayName(),
                testPlayLaunch.getLaunchId(), true);

        // Mimicking an automatic Launch creation
        testPlayLaunch = playProxy.queueNewLaunchByPlayAndChannel(playCreationHelper.getCustomerSpace(), play.getName(),
                channel.getId(), null, null, null, null, true);
        Assert.assertNotNull(testPlayLaunch.getAccountsSelected());
        Assert.assertNotNull(testPlayLaunch.getAccountsLaunched());
        Assert.assertNotNull(testPlayLaunch.getContactsLaunched());
        Assert.assertNotNull(testPlayLaunch.getAccountsErrored());
        Assert.assertNotNull(testPlayLaunch.getAccountsSuppressed());
        Assert.assertEquals(testPlayLaunch.getCreatedBy(), serviceUser);
        Assert.assertEquals(testPlayLaunch.getUpdatedBy(), serviceUser);
        Assert.assertEquals(testPlayLaunch.getLaunchState(), LaunchState.Queued);
        totalRatedAccounts = testPlayLaunch.getAccountsSelected();

        playProxy.deletePlayLaunch(playCreationHelper.getCustomerSpace(), playCreationHelper.getPlayName(),
                testPlayLaunch.getLaunchId(), true);
    }

    @Test(groups = "deployment-app", dependsOnMethods = { "testCrud" })
    public void createPlayLaunch() {
        playCreationHelper.createPlayLaunch(testPlaySetupConfig);
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
                0L, (totalRatedAccounts - 8L - 0L), 0L, 0L);

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

    public RatingEngine getRatingEngine() {
        return this.ratingEngine;
    }
}
