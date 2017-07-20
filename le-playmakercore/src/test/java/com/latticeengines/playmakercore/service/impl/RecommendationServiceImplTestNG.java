package com.latticeengines.playmakercore.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.playmakercore.Recommendation;
import com.latticeengines.domain.exposed.playmakercore.SynchronizationDestinationEnum;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.playmakercore.service.RecommendationService;
import com.latticeengines.security.exposed.service.TenantService;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-playmakercore-context.xml" })
public class RecommendationServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private RecommendationService recommendationService;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private Recommendation recommendation;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private Date CURRENT_DATE = new Date(System.currentTimeMillis());

    private String PLAY_ID = "play__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID = "launch__" + CURRENT_TIME_MILLIS;
    private String ACCOUNT_ID = "account__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_DESCRIPTION = "Recommendation done on " + CURRENT_TIME_MILLIS;
    private long TENANT_PID = 1L;
    private String CUSTOMER_SPACE = "LocalTest.LocalTest.Production";

    private Tenant tenant;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        recommendation = new Recommendation();
        recommendation.setDescription(LAUNCH_DESCRIPTION);
        recommendation.setLaunchId(LAUNCH_ID);
        recommendation.setLaunchDate(CURRENT_DATE);
        recommendation.setPlayId(PLAY_ID);
        recommendation.setAccountId(ACCOUNT_ID);
        recommendation.setLeAccountExternalID(ACCOUNT_ID);
        recommendation.setTenantId(TENANT_PID);
        recommendation.setSynchronizationDestination( //
                SynchronizationDestinationEnum.SFDC.name());

        tenant = new Tenant(CUSTOMER_SPACE);

        MultiTenantContext.setTenant(tenant);
    }

    @AfterClass(groups = "functional")
    public void teardown() throws Exception {
    }

    @Test(groups = "functional")
    public void testGetPreCreate() {
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetPreCreate" })
    public void testCreateRecommendation() {
        recommendationService.create(recommendation);
        Assert.assertNotNull(recommendation.getRecommendationId());
        Assert.assertNotNull(recommendation.getPid());
    }

    @Test(groups = "functional", dependsOnMethods = { "testCreateRecommendation" })
    public void testGetRecommendationByLaunchId() {
        List<Recommendation> recommendations = recommendationService.findByLaunchId(LAUNCH_ID);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByLaunchId" })
    public void testGetRecommendationByPlayId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(PLAY_ID);

        int recommendationCount = recommendationService.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds);

        Assert.assertEquals(1, recommendationCount);

        List<Recommendation> recommendations = recommendationService.findRecommendations(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationByPlayId" })
    public void testGetRecommendationAsMapByPlayId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        List<String> playIds = new ArrayList<>();
        playIds.add(PLAY_ID);

        int recommendationCount = recommendationService.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), playIds);

        Assert.assertEquals(1, recommendationCount);

        List<Map<String, Object>> recommendations = recommendationService.findRecommendationsAsMap(new Date(0L), //
                0, recommendationCount, SynchronizationDestinationEnum.SFDC.toString(), playIds);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), recommendationCount);

        for (Map<String, Object> recommendation : recommendations) {
            Assert.assertTrue(recommendation.size() > 0);
        }
    }

    @Test(groups = "functional", dependsOnMethods = { "testGetRecommendationAsMapByPlayId" })
    public void testGetRecommendationWithoutPlayId() {

        tenant = new Tenant(CUSTOMER_SPACE);
        MultiTenantContext.setTenant(tenant);

        int recommendationCount = recommendationService.findRecommendationCount(new Date(0L), //
                SynchronizationDestinationEnum.SFDC.toString(), null);

        Assert.assertTrue(recommendationCount > 0);

        int minPageSize = Math.min(10, recommendationCount);

        List<Recommendation> recommendations = recommendationService.findRecommendations(new Date(0L), //
                (recommendationCount - minPageSize), minPageSize, SynchronizationDestinationEnum.SFDC.toString(), null);
        Assert.assertNotNull(recommendations);
        Assert.assertTrue(recommendations.size() > 0);
        Assert.assertEquals(recommendations.size(), minPageSize);

        for (Recommendation recommendation : recommendations) {
            Assert.assertNotNull(recommendation.getRecommendationId());
            Assert.assertNotNull(recommendation.getPid());
            Assert.assertNotNull(recommendation.getTenantId());
            Assert.assertEquals(recommendation.getTenantId().longValue(), TENANT_PID);
        }
    }
}
