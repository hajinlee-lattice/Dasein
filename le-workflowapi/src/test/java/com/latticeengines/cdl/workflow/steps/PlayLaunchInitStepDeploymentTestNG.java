package com.latticeengines.cdl.workflow.steps;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.cdl.workflow.steps.play.PlayLaunchInitStepTestHelper;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemName;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.pls.RatingBucketName;
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
import com.latticeengines.testframework.exposed.domain.TestPlayChannelConfig;
import com.latticeengines.testframework.exposed.domain.TestPlaySetupConfig;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.testframework.service.impl.TestPlayCreationHelper;
import com.latticeengines.yarn.exposed.service.JobService;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:playmakercore-context.xml",
        "classpath:test-playlaunch-properties-context.xml", "classpath:yarn-context.xml", "classpath:proxy-context.xml",
        "classpath:test-workflowapi-context.xml", "classpath:test-testframework-cleanup-context.xml" })
public class PlayLaunchInitStepDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStepDeploymentTestNG.class);

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

    @Autowired
    RecommendationService recommendationService;

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

    @Autowired
    private DataCollectionProxy dataCollectionProxy;

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

    String randId = UUID.randomUUID().toString();

    private Tenant tenant;

    private Play rulesBasedPlay;

    private PlayLaunch rulesBasedPlayLaunch;

    private CustomerSpace customerSpace;

    private Long topNCount;

    private List<PlayType> types;

    @BeforeClass(groups = "deployment")
    public void setup() throws Exception {
        topNCount = 1500L;

        TestPlaySetupConfig testPlaySetupConfig = new TestPlaySetupConfig.Builder()
                .addChannel(new TestPlayChannelConfig.Builder().excludeItemsWithoutSalesforceId(true)
                        .bucketsToLaunch(new HashSet<>(Arrays.asList(RatingBucketName.values())))
                        .destinationSystemType(CDLExternalSystemType.MAP)
                        .destinationSystemName(CDLExternalSystemName.Marketo)
                        .destinationSystemId("Marketo_" + System.currentTimeMillis())
                        .trayAuthenticationId(UUID.randomUUID().toString()).topNCount(topNCount).build())
                .playLaunchDryRun(true).build();

        testPlayCreationHelper.setupTenantAndCreatePlay(testPlaySetupConfig);
        testPlayCreationHelper.createPlayLaunch(testPlaySetupConfig);
        testPlayCreationHelper.launchPlayWorkflow(testPlaySetupConfig);

        tenant = testPlayCreationHelper.getTenant();
        rulesBasedPlay = testPlayCreationHelper.getPlay();
        rulesBasedPlayLaunch = testPlayCreationHelper.getPlayLaunch();

        // Update channelId types so that
        types = playProxy.getPlayTypes(tenant.getId());
        Assert.assertNotNull(types);
        rulesBasedPlay.setPlayType(types.get(1));
        playProxy.createOrUpdatePlay(tenant.getId(), rulesBasedPlay);

        String playId = rulesBasedPlay.getName();
        String playLaunchId = rulesBasedPlayLaunch.getId();
        long pageSize = 20L;

        MockitoAnnotations.initMocks(this);

        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        helper = new PlayLaunchInitStepTestHelper(playProxy, lookupIdMappingProxy, entityProxy, recommendationService,
                pageSize, metadataProxy, sqoopProxy, ratingEngineProxy, jobService, dataCollectionProxy, dataDbDriver,
                dataDbUrl, dataDbUser, dataDbPassword, dataDbDialect, dataDbType, yarnConfiguration);

        playLaunchInitStep = new PlayLaunchInitStep();
        playLaunchInitStep.setPlayLaunchProcessor(helper.getPlayLaunchProcessor());
        playLaunchInitStep.setPlayProxy(playProxy);
        playLaunchInitStep.setTenantEntityMgr(tenantEntityMgr);
        playLaunchInitStep.setExecutionContext(new ExecutionContext());

        customerSpace = CustomerSpace.parse(tenant.getId());
        playLaunchInitStep.setConfiguration(createConf(customerSpace, playId, playLaunchId));
    }

    @AfterClass(groups = { "deployment" })
    public void teardown() {
        testPlayCreationHelper.cleanupArtifacts(true);

        List<Recommendation> recommendations = recommendationService.findByLaunchId(rulesBasedPlayLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);

        recommendations.forEach(rec -> {
            log.info("Cleaning up recommendation: " + rec.getId());
            recommendationService.delete(rec, false);
        });
        recommendations = recommendationService.findByLaunchId(rulesBasedPlayLaunch.getId());

        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 0);
    }

    // @Test(groups = "deployment")
    public void testCrossSellPlayLaunch() {
        Assert.assertEquals(rulesBasedPlayLaunch.getLaunchState(), LaunchState.Launching);
        Assert.assertNotNull(rulesBasedPlayLaunch.getAccountsSelected());
        Assert.assertTrue(rulesBasedPlayLaunch.getAccountsSelected() > 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getAccountsLaunched().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getContactsLaunched().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getAccountsErrored().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getAccountsSuppressed().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getLaunchCompletionPercent(), 0.0D);

        List<Recommendation> recommendations = recommendationService.findByLaunchId(rulesBasedPlayLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 0);

        playLaunchInitStep.execute();

        PlayLaunch updatedPlayLaunch = playProxy.getPlayLaunch(customerSpace.toString(), rulesBasedPlay.getName(),
                rulesBasedPlayLaunch.getId());

        Assert.assertNotNull(updatedPlayLaunch);
        Assert.assertEquals(updatedPlayLaunch.getLaunchState(), LaunchState.Launched);
        Assert.assertTrue(updatedPlayLaunch.getAccountsLaunched() > 0);
        Assert.assertEquals(updatedPlayLaunch.getLaunchCompletionPercent(), 100.0D);
    }

    @Test(groups = "deployment")
    public void testRuleBasedPlayLaunch() {
        Assert.assertEquals(rulesBasedPlayLaunch.getLaunchState(), LaunchState.Launching);
        Assert.assertNotNull(rulesBasedPlayLaunch.getAccountsSelected());
        Assert.assertTrue(rulesBasedPlayLaunch.getAccountsSelected() > 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getAccountsLaunched().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getContactsLaunched().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getAccountsErrored().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getAccountsSuppressed().longValue(), 0L);
        Assert.assertEquals(rulesBasedPlayLaunch.getLaunchCompletionPercent(), 0.0D);

        List<Recommendation> recommendations = recommendationService.findByLaunchId(rulesBasedPlayLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 0);

        playLaunchInitStep.execute();

        PlayLaunch updatedPlayLaunch = playProxy.getPlayLaunch(customerSpace.toString(), rulesBasedPlay.getName(),
                rulesBasedPlayLaunch.getId());

        Assert.assertNotNull(updatedPlayLaunch);
        Assert.assertEquals(updatedPlayLaunch.getLaunchState(), LaunchState.Launched);
        Assert.assertTrue(updatedPlayLaunch.getAccountsLaunched() > 0);
        Assert.assertEquals(updatedPlayLaunch.getLaunchCompletionPercent(), 100.0D);
    }

    @Test(groups = "deployment", dependsOnMethods = { "testRuleBasedPlayLaunch" })
    public void verifyRecommendationsDirectly() {
        List<Recommendation> recommendations = recommendationService.findByLaunchId(rulesBasedPlayLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        AtomicInteger contactCounts = new AtomicInteger();
        Set<String> accountIds = ConcurrentHashMap.newKeySet();

        recommendations.forEach(rec -> {
            Assert.assertNotNull(rec.getAccountId());
            Assert.assertNotNull(rec.getId());
            Assert.assertNotNull(rec.getLastUpdatedTimestamp());
            Assert.assertNotNull(rec.getLaunchDate());
            Assert.assertNotNull(rec.getLaunchId());
            Assert.assertNotNull(rec.getLeAccountExternalID());
            Assert.assertNotNull(rec.getPid());
            Assert.assertNotNull(rec.getPlayId());
            Assert.assertNotNull(rec.getTenantId());
            Assert.assertNotNull(rec.getDestinationOrgId());
            Assert.assertNotNull(rec.getDestinationSysType());
            Assert.assertEquals(rec.getDestinationOrgId(), rulesBasedPlayLaunch.getDestinationOrgId());
            Assert.assertEquals(CDLExternalSystemType.valueOf(rec.getDestinationSysType()),
                    rulesBasedPlayLaunch.getDestinationSysType());
            Assert.assertNull(rec.getMonetaryValue());

            Assert.assertEquals(rec.getPlayId(), rulesBasedPlay.getName());
            Assert.assertEquals(rec.getLaunchId(), rulesBasedPlayLaunch.getId());
            if (CollectionUtils.isNotEmpty(rec.getExpandedContacts())) {
                contactCounts.addAndGet(rec.getExpandedContacts().size());
            }

            Assert.assertFalse(accountIds.contains(rec.getAccountId()));
            accountIds.add(rec.getAccountId());
        });

        Assert.assertTrue(contactCounts.get() > 0);
    }

    private PlayLaunchInitStepConfiguration createConf(CustomerSpace customerSpace, String playName,
            String playLaunchId) {
        PlayLaunchInitStepConfiguration config = new PlayLaunchInitStepConfiguration();
        config.setCustomerSpace(customerSpace);
        config.setPlayLaunchId(playLaunchId);
        config.setPlayName(playName);
        return config;
    }
}
