package com.latticeengines.playmaker.service.impl;

import java.util.Arrays;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.pls.service.impl.TestPlayCreationHelper;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml", "classpath:playmakercore-context.xml",
        "classpath:test-playmaker-context.xml" })
public class LpiPMPlayImplDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(LpiPMPlayImplDeploymentTestNG.class);

    private LpiPMPlayImpl lpiPMPlayImpl;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    @Autowired
    private PlayProxy playProxy;

    private long accountCount;

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
        testPlayCreationHelper.setupTenant();

        lpiPMPlayImpl = new LpiPMPlayImpl();
        lpiPMPlayImpl.setPlayProxy(playProxy);
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
        testPlayCreationHelper.createPlayLaunch();

        int playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 1);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testGetPlayCountAfterCreatingPlayWithLaunch" })
    public void testGetPlayCountAfterCreatingAnotherPlayWithoutLaunch() throws Exception {
        testPlayCreationHelper.createPlayOnly();

        int playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 1);

        testPlayCreationHelper.createPlayLaunch();

        playCount = lpiPMPlayImpl.getPlayCount(0, null);
        Assert.assertEquals(playCount, 2);
    }
}
