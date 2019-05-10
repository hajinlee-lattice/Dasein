package com.latticeengines.playmaker.service.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml",
        "classpath:playmakercore-context.xml", "classpath:test-playmaker-context.xml" })
public class LpiPMPlayImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    @Inject
    private LpiPMPlayImpl lpiPMPlayImpl;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    @Inject
    private PlayProxy playProxy;

    private Play firstPlayWithLaunch;
    private Play secondPlayWithLaunch;
    private Tenant tenant;

    private PlayLaunchConfig playLaunchConfig = null;

    @BeforeClass(groups = "deployment")
    public void setup() {
        playLaunchConfig = new PlayLaunchConfig.Builder().build();
        testPlayCreationHelper.setupTenantAndData();
    }

    @Test(groups = "deployment")
    public void testGetPlayCountWithoutPlayCreation() {
        int playCount = lpiPMPlayImpl.getPlayCount(0, null, 1, null);
        Assert.assertEquals(playCount, 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountWithoutPlayCreation" })
    public void testGetPlayCountAfterCreatingPlayWithoutLaunch() throws Exception {
        PlayLaunchConfig plConfig = new PlayLaunchConfig.Builder().build();
        testPlayCreationHelper.setupRatingEngineAndSegment();
        testPlayCreationHelper.createPlay(plConfig);

        int playCount = lpiPMPlayImpl.getPlayCount(0, null, 1, null);
        Assert.assertEquals(playCount, 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountAfterCreatingPlayWithoutLaunch" })
    public void testGetPlayCountAfterCreatingPlayWithLaunch() {
        tenant = testPlayCreationHelper.getTenant();
        testPlayCreationHelper.createPlayLaunch(playLaunchConfig);
        firstPlayWithLaunch = testPlayCreationHelper.getPlay();
        PlayLaunch firstPlayLaunch = testPlayCreationHelper.getPlayLaunch();
        playProxy.updatePlayLaunch(tenant.getId(), firstPlayWithLaunch.getName(), firstPlayLaunch.getLaunchId(),
                LaunchState.Launching);
        playProxy.updatePlayLaunch(tenant.getId(), firstPlayWithLaunch.getName(), firstPlayLaunch.getLaunchId(),
                LaunchState.Launched);

        Map<String, String> org = new HashMap<String, String>();
        org = testPlayCreationHelper.getOrgInfo();
        int playCount = lpiPMPlayImpl.getPlayCount(0, null, 1, org);
        Assert.assertEquals(playCount, 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountAfterCreatingPlayWithLaunch" })
    public void testGetPlayCountAfterCreatingAnotherPlayWithoutLaunch() throws Exception {
        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        Play secondPlay = testPlayCreationHelper.createPlayOnlyAndGet();

        int playCount = lpiPMPlayImpl.getPlayCount(0, null, 1, null);
        Assert.assertEquals(playCount, 1);

        testPlayCreationHelper.createPlayLaunch(playLaunchConfig);
        PlayLaunch secondPlayLaunch = testPlayCreationHelper.getPlayLaunch();
        playProxy.updatePlayLaunch(tenant.getId(), secondPlay.getName(), secondPlayLaunch.getLaunchId(),
                LaunchState.Launching);
        playProxy.updatePlayLaunch(tenant.getId(), secondPlay.getName(), secondPlayLaunch.getLaunchId(),
                LaunchState.Launched);
        playCount = lpiPMPlayImpl.getPlayCount(0, null, 1, null);
        Assert.assertEquals(playCount, 2);
        secondPlayWithLaunch = secondPlay;
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountAfterCreatingAnotherPlayWithoutLaunch" })
    public void testGetPlayCountForFutureTimestamp() throws Exception {

        System.out.println("firstPlayWithLaunch.getUpdated() = " + firstPlayWithLaunch.getUpdated());
        System.out.println("lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch) = "
                + lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch));

        System.out.println("secondPlayWithLaunch.getUpdated() = " + secondPlayWithLaunch.getUpdated());
        System.out.println("lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch) = "
                + lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch));

        int playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch), null, 1, null);
        Assert.assertEquals(playCount, 2);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch) - 2, null, 1, null);
        Assert.assertEquals(playCount, 2);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch) + 2, null, 1, null);
        Assert.assertEquals(playCount, 1);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch) - 2, null, 1, null);
        Assert.assertEquals(playCount, 1);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch), null, 1, null);
        Assert.assertEquals(playCount, 1);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch) + 2, null, 1, null);
        Assert.assertEquals(playCount, 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountForFutureTimestamp" })
    public void testDeletePlay() {
        playProxy.deletePlay(tenant.getId(), secondPlayWithLaunch.getName(), false);
        int playCount = lpiPMPlayImpl.getPlayCount(0, null, 1, null);
        Assert.assertEquals(playCount, 1);
    }
}
