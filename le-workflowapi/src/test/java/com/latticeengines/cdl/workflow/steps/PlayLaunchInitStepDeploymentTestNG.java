package com.latticeengines.cdl.workflow.steps;

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
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.pls.service.impl.TestPlayCreationHelper;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;
import com.latticeengines.testframework.service.impl.GlobalAuthCleanupTestListener;
import com.latticeengines.yarn.exposed.service.JobService;

@Listeners({ GlobalAuthCleanupTestListener.class })
@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml", "classpath:playmakercore-context.xml",
        "classpath:test-playlaunch-properties-context.xml", "classpath:yarn-context.xml",
        "classpath:proxy-context.xml", "classpath:test-workflowapi-context.xml" })
public class PlayLaunchInitStepDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private static final Logger log = LoggerFactory.getLogger(PlayLaunchInitStepDeploymentTestNG.class);

    private PlayLaunchInitStep playLaunchInitStep;

    private PlayLaunchInitStepTestHelper helper;

    @Mock
    PlayLaunchInitStepConfiguration configuration;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    RecommendationService recommendationService;

    @Autowired
    private MetadataProxy metadataProxy;

    @Autowired
    private SqoopProxy sqoopProxy;

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

    String randId = UUID.randomUUID().toString();

    private Tenant tenant;

    private Play play;

    private PlayLaunch playLaunch;

    private CustomerSpace customerSpace;

    @BeforeClass(groups = "workflow")
    public void setup() throws Exception {
        testPlayCreationHelper.setupTenantAndCreatePlay();

        tenant = testPlayCreationHelper.getTenant();
        play = testPlayCreationHelper.getPlay();
        playLaunch = testPlayCreationHelper.getPlayLaunch();

        String playId = play.getName();
        String playLaunchId = playLaunch.getId();
        long pageSize = 20L;

        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        MockitoAnnotations.initMocks(this);

        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        helper = new PlayLaunchInitStepTestHelper(internalResourceRestApiProxy, entityProxy, recommendationService,
                pageSize, metadataProxy, sqoopProxy, jobService, dataDbDriver, dataDbUrl, dataDbUser, dataDbPassword,
                dataDbDialect, dataDbType, yarnConfiguration);

        playLaunchInitStep = new PlayLaunchInitStep();
        playLaunchInitStep.setPlayLaunchProcessor(helper.getPlayLaunchProcessor());
        playLaunchInitStep.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
        playLaunchInitStep.setTenantEntityMgr(tenantEntityMgr);

        customerSpace = CustomerSpace.parse(tenant.getId());
        playLaunchInitStep.setConfiguration(createConf(customerSpace, playId, playLaunchId));
    }

    @AfterClass(groups = { "workflow" })
    public void teardown() throws Exception {

        testPlayCreationHelper.cleanupArtifacts();

        List<Recommendation> recommendations = recommendationService.findByLaunchId(playLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);

        recommendations.stream().forEach(rec -> {
            log.info("Cleaning up recommendation: " + rec.getId());
            recommendationService.delete(rec);
        });
    }

    @Test(groups = "workflow")
    public void testExecute() {
        Assert.assertEquals(playLaunch.getLaunchState(), LaunchState.Launching);
        Assert.assertNull(playLaunch.getAccountsLaunched());
        Assert.assertEquals(playLaunch.getLaunchCompletionPercent(), 0.0D);

        List<Recommendation> recommendations = recommendationService.findByLaunchId(playLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertEquals(recommendations.size(), 0);

        playLaunchInitStep.execute();

        PlayLaunch updatedPlayLaunch = internalResourceRestApiProxy.getPlayLaunch(customerSpace, play.getName(),
                playLaunch.getId());

        Assert.assertNotNull(updatedPlayLaunch);
        Assert.assertEquals(updatedPlayLaunch.getLaunchState(), LaunchState.Launched);
        Assert.assertTrue(updatedPlayLaunch.getAccountsLaunched() > 0);
        Assert.assertEquals(updatedPlayLaunch.getLaunchCompletionPercent(), 100.0D);

    }

    @Test(groups = "workflow", dependsOnMethods = { "testExecute" })
    public void verifyRecommendationsDirectly() {
        List<Recommendation> recommendations = recommendationService.findByLaunchId(playLaunch.getId());
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        AtomicInteger contactCounts = new AtomicInteger();
        Set<String> accountIds = ConcurrentHashMap.newKeySet();

        recommendations.stream().forEach(rec -> {
            Assert.assertNotNull(rec.getAccountId());
            Assert.assertNotNull(rec.getId());
            Assert.assertNotNull(rec.getLastUpdatedTimestamp());
            Assert.assertNotNull(rec.getLaunchDate());
            Assert.assertNotNull(rec.getLaunchId());
            Assert.assertNotNull(rec.getLeAccountExternalID());
            Assert.assertNotNull(rec.getPid());
            Assert.assertNotNull(rec.getPlayId());
            Assert.assertNotNull(rec.getTenantId());

            Assert.assertEquals(rec.getPlayId(), play.getName());
            Assert.assertEquals(rec.getLaunchId(), playLaunch.getId());
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
