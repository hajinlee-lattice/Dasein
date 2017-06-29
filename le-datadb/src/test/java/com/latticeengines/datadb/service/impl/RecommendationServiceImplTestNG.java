package com.latticeengines.datadb.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.support.DirtiesContextTestExecutionListener;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.datadb.service.RecommendationService;
import com.latticeengines.domain.exposed.datadb.Recommendation;
import com.latticeengines.domain.exposed.pls.Play;
import com.latticeengines.security.exposed.service.TenantService;

@TestExecutionListeners({ DirtiesContextTestExecutionListener.class })
@ContextConfiguration(locations = { "classpath:test-datadb-context.xml" })
public class RecommendationServiceImplTestNG extends AbstractTestNGSpringContextTests {

    @Autowired
    private RecommendationService recommendationService;

    @Autowired
    private TenantService tenantService;

    private Play play;

    private Recommendation recommendation;

    private long CURRENT_TIME_MILLIS = System.currentTimeMillis();

    private String PLAY_ID = "play__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_ID = "launch__" + CURRENT_TIME_MILLIS;
    private String LAUNCH_DESCRIPTION = "Recommendation done on " + CURRENT_TIME_MILLIS;
    private long TENANT_PID = 1L;

    @BeforeClass(groups = "functional")
    public void setup() throws Exception {

        recommendation = new Recommendation();
        recommendation.setDescription(LAUNCH_DESCRIPTION);
        recommendation.setLaunchId(LAUNCH_ID);
        recommendation.setPlayId(PLAY_ID);
        recommendation.setTenantId(TENANT_PID);
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
}
