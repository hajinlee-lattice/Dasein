package com.latticeengines.cdl.workflow;

import static org.testng.Assert.assertNotNull;

import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.workflow.JobStatus;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.testframework.exposed.domain.PlayLaunchConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;

public class PlayLaunchWorkflowDeploymentTestNG extends CDLWorkflowDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchWorkflowDeploymentTestNG.class);

    @Autowired
    private PlayProxy playProxy;

    @Autowired
    RecommendationService recommendationService;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    String randId = UUID.randomUUID().toString();

    private Play defaultPlay;

    private PlayLaunch defaultPlayLaunch;

    PlayLaunchConfig playLaunchConfig = null;

    @Override
    public Tenant currentTestTenant() {
        return testPlayCreationHelper.getTenant();
    }

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        String existingTenant = null;//"LETest1546299140564";

        playLaunchConfig = new PlayLaunchConfig.Builder()
                .existingTenant(existingTenant)
                .mockRatingTable(false)
                .testPlayCrud(false)
                .destinationSystemType(CDLExternalSystemType.MAP)
                .destinationSystemId("Marketo_"+System.currentTimeMillis())
                .topNCount(1500L)
                .build(); 

        testPlayCreationHelper.setupTenantAndCreatePlay(playLaunchConfig);

        super.deploymentTestBed = testPlayCreationHelper.getDeploymentTestBed();
        
        defaultPlay = testPlayCreationHelper.getPlay();
        defaultPlayLaunch = testPlayCreationHelper.getPlayLaunch();

    }

    @Test(groups = "deployment")
    public void testPlayLaunchWorkflow() {
        log.info("Submitting PlayLaunch Workflow: " + defaultPlayLaunch);
        defaultPlayLaunch = testPlayCreationHelper.launchPlayWorkflow(playLaunchConfig);
        assertNotNull(defaultPlayLaunch);
        assertNotNull(defaultPlayLaunch.getApplicationId());
        log.info(String.format("PlayLaunch Workflow application id is %s", defaultPlayLaunch.getApplicationId()));

        JobStatus completedStatus = waitForWorkflowStatus(defaultPlayLaunch.getApplicationId(), false);
        Assert.assertEquals(completedStatus, JobStatus.COMPLETED);
    }

    @AfterClass(groups = "deployment")
    public void tearDown() throws Exception {
        
    }

}
