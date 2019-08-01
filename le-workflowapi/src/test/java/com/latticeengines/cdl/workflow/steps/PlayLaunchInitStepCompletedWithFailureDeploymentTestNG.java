package com.latticeengines.cdl.workflow.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.play.PlayLaunchInitStepTestHelper;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.cdl.play.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import com.latticeengines.proxy.exposed.cdl.RatingEngineProxy;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;
import com.latticeengines.yarn.exposed.service.JobService;

// TODO - enable when we have a way to simulate full/partial failure
//
//@Listeners({ GlobalAuthCleanupTestListener.class })
//@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
//@ContextConfiguration(locations = { "classpath:test-pls-context.xml", "classpath:playmakercore-context.xml",
//        "classpath:test-playlaunch-properties-context.xml", "classpath:yarn-context.xml" })
public class PlayLaunchInitStepCompletedWithFailureDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private PlayLaunchInitStep playLaunchInitStep;

    private PlayLaunchInitStepTestHelper helper;

    @Mock
    PlayLaunchInitStepConfiguration configuration;

    @Value("${common.test.pls.url}")
    private String internalResourceHostPort;

    @Autowired
    private PlayProxy playProxy;

    @Autowired
    private LookupIdMappingProxy lookupIdMappingProxy;

    @Mock
    RecommendationService partiallyBadRecommendationService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SqoopProxy sqoopProxy;

    @Autowired
    private RatingEngineProxy ratingEngineProxy;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    private JobService jobService;

    @Value("${datadb.datasource.driver}")
    private String dataDbDriver;

    @Value("${datadb.datasource.sqoop.url}")
    private String dataDbUrl;

    @Value("${datadb.datasource.user}")
    private String dataDbUser;

    @Value("${datadb.datasource.password.encrypted}")
    private String dataDbPassword;

    @Value("${datadb.datasource.dialect}")
    private String dataDbDialect;

    @Value("${datadb.datasource.type}")
    private String dataDbType;

    @Autowired
    TenantEntityMgr tenantEntityMgr;

    @Autowired
    private TestPlayCreationHelper testPlayCreationHelper;

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private CustomerSpace customerSpace;

    // TODO - enable when we have a way to simulate full/partial failure
    @BeforeClass(groups = "deployment", enabled = false)
    public void setup() throws Exception {
        final TestPlaySetupConfig testPlaySetupConfig = new TestPlaySetupConfig.Builder().build();
        testPlayCreationHelper.setupTenantAndCreatePlay(testPlaySetupConfig);

        tenant = testPlayCreationHelper.getTenant();
        play = testPlayCreationHelper.getPlay();
        playLaunch = testPlayCreationHelper.getPlayLaunch();

        String playId = play.getName();
        String playLaunchId = playLaunch.getId();
        long pageSize = 20L;

        MockitoAnnotations.initMocks(this);

        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        // setting partially bad recommendation service to simulate partial
        // failure during recommendation creation/saving which should not cause
        // play launch to go in FAILED state. Ensure that play launch state is
        // LAUNCHED in case of partial failure
        mockPartiallyBadRecommendationService();

        helper = new PlayLaunchInitStepTestHelper(playProxy, lookupIdMappingProxy, entityProxy,
                partiallyBadRecommendationService, pageSize, metadataProxy, sqoopProxy, ratingEngineProxy, jobService,
                dataCollectionProxy, dataDbDriver, dataDbUrl, dataDbUser, dataDbPassword, dataDbDialect, dataDbType,
                yarnConfiguration);

        playLaunchInitStep = new PlayLaunchInitStep();
        playLaunchInitStep.setPlayLaunchProcessor(helper.getPlayLaunchProcessor());
        playLaunchInitStep.setPlayProxy(playProxy);
        playLaunchInitStep.setTenantEntityMgr(tenantEntityMgr);

        customerSpace = CustomerSpace.parse(tenant.getId());
        playLaunchInitStep.setConfiguration(createConf(customerSpace, playId, playLaunchId));
    }

    // TODO - enable when we have a way to simulate full/partial failure
    @AfterClass(groups = { "deployment" }, enabled = false)
    public void teardown() {

        testPlayCreationHelper.cleanupArtifacts(true);
    }

    // TODO - enable when we have a way to simulate full/partial failure
    @Test(groups = "deployment", enabled = false)
    public void testExecute() {
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Launching);
        Assert.assertNull(playLaunch.getAccountsLaunched());
        Assert.assertEquals(playLaunch.getLaunchCompletionPercent(), 0.0D);

        playLaunchInitStep.execute();
        PlayLaunch updatedPlayLaunch = playProxy.getPlayLaunch(customerSpace.toString(), play.getName(),
                playLaunch.getId());

        Assert.assertNotNull(updatedPlayLaunch);
        Assert.assertEquals(updatedPlayLaunch.getLaunchState(), LaunchState.Launched);
        Assert.assertTrue(updatedPlayLaunch.getAccountsLaunched() > 0L);
        Assert.assertTrue(updatedPlayLaunch.getAccountsLaunched() > 0L);
        Assert.assertEquals(updatedPlayLaunch.getAccountsErrored().longValue(), 1L);
        Assert.assertEquals(updatedPlayLaunch.getLaunchCompletionPercent(), 100.0D);
    }

    private PlayLaunchInitStepConfiguration createConf(CustomerSpace customerSpace, String playName,
            String playLaunchId) {
        PlayLaunchInitStepConfiguration config = new PlayLaunchInitStepConfiguration();
        config.setCustomerSpace(customerSpace);
        config.setPlayLaunchId(playLaunchId);
        config.setPlayName(playName);
        return config;
    }

    private void mockPartiallyBadRecommendationService() {
        // throw exception in first create call then success (doing nothing)
        // afterwards to simulate partial failure
        doThrow(new RuntimeException()) //
                .doNothing() //
                .when(partiallyBadRecommendationService) //
                .create(any(Recommendation.class));
    }
}
