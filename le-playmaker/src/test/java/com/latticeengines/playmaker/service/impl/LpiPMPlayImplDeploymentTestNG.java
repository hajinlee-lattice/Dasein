package com.latticeengines.playmaker.service.impl;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-testframework-cleanup-context.xml",
        "classpath:playmakercore-context.xml", "classpath:test-playmaker-context.xml" })
public class LpiPMPlayImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(LpiPMPlayImplDeploymentTestNG.class);

    @Inject
    private LpiPMPlayImpl lpiPMPlayImpl;

    @Inject
    private TestPlayCreationHelper testPlayCreationHelper;

    private long accountCount;

    private Play firstPlayWithLaunch;
    private Play secondPlayWithLaunch;

    List<String> accountFields = Arrays.asList( //
            "LatticeAccountId", //
            "CDLUpdatedTime", //
            "AccountId", //
            "Website", //
            "LDC_Name", //
            "ID", //
            "LEAccountExternalID", //
            "LastModificationDate", //
            "SalesforceAccountID", //
            "SfdcAccountID", //
            "RowNum");

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndData();
    }

    @Test(groups = "deployment")
    public void testGetPlayCountWithoutPlayCreation() {
        int playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountWithoutPlayCreation" })
    public void testGetPlayCountAfterCreatingPlayWithoutLaunch() throws Exception {
        testPlayCreationHelper.setupPlayTestEnv();
        testPlayCreationHelper.createPlay();

        int playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 0);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountAfterCreatingPlayWithoutLaunch" })
    public void testGetPlayCountAfterCreatingPlayWithLaunch() throws Exception {
        testPlayCreationHelper.createPlayLaunch(true);
        firstPlayWithLaunch = testPlayCreationHelper.getPlay();
        int playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountAfterCreatingPlayWithLaunch" })
    public void testGetPlayCountAfterCreatingAnotherPlayWithoutLaunch() throws Exception {
        Thread.sleep(TimeUnit.SECONDS.toMillis(4));
        Play secondPlay = testPlayCreationHelper.createPlayOnlyAndGet();

        int playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 1);

        testPlayCreationHelper.createPlayLaunch(true);
        playCount = lpiPMPlayImpl.getPlayCount(0, null);
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

        int playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch), null);
        Assert.assertEquals(playCount, 2);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch) - 2, null);
        Assert.assertEquals(playCount, 2);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(firstPlayWithLaunch) + 2, null);
        Assert.assertEquals(playCount, 1);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch) - 2, null);
        Assert.assertEquals(playCount, 1);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch), null);
        Assert.assertEquals(playCount, 1);
        playCount = lpiPMPlayImpl.getPlayCount(lpiPMPlayImpl.secondsFromEpoch(secondPlayWithLaunch) + 2, null);
        Assert.assertEquals(playCount, 0);
    }
}
