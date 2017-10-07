package com.latticeengines.leadprioritization.workflow.steps;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;

import java.util.List;
import java.util.UUID;

import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.pls.LaunchState;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.pls.PlayLaunch;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps.PlayLaunchInitStepConfiguration;
import com.latticeengines.leadprioritization.workflow.steps.play.PlayLaunchInitStepTestHelper;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.pls.service.impl.TestPlayCreationHelper;
import com.latticeengines.proxy.exposed.dante.DanteLeadProxy;
import com.latticeengines.proxy.exposed.objectapi.EntityProxy;
import com.latticeengines.proxy.exposed.pls.InternalResourceRestApiProxy;
import com.latticeengines.security.exposed.entitymanager.TenantEntityMgr;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-pls-context.xml", "classpath:playmakercore-context.xml",
        "classpath:test-playlaunch-properties-context.xml" })
public class PlayLaunchInitStepDeploymentTestNG extends AbstractTestNGSpringContextTests {

    private PlayLaunchInitStep playLaunchInitStep;

    private PlayLaunchInitStepTestHelper helper;

    @Mock
    PlayLaunchInitStepConfiguration configuration;

    @Value("${common.pls.url}")
    private String internalResourceHostPort;

    private InternalResourceRestApiProxy internalResourceRestApiProxy;

    @Autowired
    RecommendationService recommendationService;

    @Mock
    DanteLeadProxy danteLeadProxy;

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
        long pageSize = 2L;

        internalResourceRestApiProxy = new InternalResourceRestApiProxy(internalResourceHostPort);

        MockitoAnnotations.initMocks(this);

        EntityProxy entityProxy = testPlayCreationHelper.initEntityProxy();

        mockDanteLeadProxy();

        helper = new PlayLaunchInitStepTestHelper(internalResourceRestApiProxy, entityProxy, recommendationService,
                danteLeadProxy, pageSize);

        playLaunchInitStep = new PlayLaunchInitStep();
        playLaunchInitStep.setPlayLaunchProcessor(helper.getPlayLaunchProcessor());
        playLaunchInitStep.setInternalResourceRestApiProxy(internalResourceRestApiProxy);
        playLaunchInitStep.setTenantEntityMgr(tenantEntityMgr);

        customerSpace = CustomerSpace.parse(tenant.getId());
        playLaunchInitStep.setConfiguration(createConf(customerSpace, playId, playLaunchId));
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
        });
    }

    private PlayLaunchInitStepConfiguration createConf(CustomerSpace customerSpace, String playName,
            String playLaunchId) {
        PlayLaunchInitStepConfiguration config = new PlayLaunchInitStepConfiguration();
        config.setCustomerSpace(customerSpace);
        config.setPlayLaunchId(playLaunchId);
        config.setPlayName(playName);
        return config;
    }

    private void mockDanteLeadProxy() {
        doNothing() //
                .when(danteLeadProxy) //
                .create(any(Recommendation.class), anyString());
    }
}