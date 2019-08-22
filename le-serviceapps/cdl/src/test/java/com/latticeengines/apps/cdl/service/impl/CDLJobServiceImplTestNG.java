package com.latticeengines.apps.cdl.service.impl;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.DefaultTypedTuple;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Listeners;
import org.testng.annotations.Test;

import com.latticeengines.apps.cdl.testframework.CDLFunctionalTestNGBase;
import com.latticeengines.domain.exposed.cdl.scheduling.SchedulingResult;
import com.latticeengines.testframework.service.impl.SimpleRetryAnalyzer;
import com.latticeengines.testframework.service.impl.SimpleRetryListener;

@Listeners({SimpleRetryListener.class})
public class CDLJobServiceImplTestNG extends CDLFunctionalTestNGBase {

    @Inject
    private CDLJobServiceImpl cdlJobService;

    @Inject
    private RedisTemplate<String, Object> sider;

    @Value("${common.le.environment}")
    private String leEnv;

    private static final String shouldStarved = "starved";
    private static final String shouldStarved1 = "starved1";
    private static final String shouldStarved2 = "starved2";
    private static final String shouldNotStarved = "not_starved";
    private static final String shouldNotStarved1 = "not_starved1";
    private static final Instant curTime = Instant.now();
    private static String MAIN_TRACKING_SET;

    @BeforeClass(groups = "functional")
    public void setupData() {
        MAIN_TRACKING_SET = leEnv + "_PA_SCHEDULER_MAIN_TRACKING_SET";
        sider.delete(MAIN_TRACKING_SET);
        Set<ZSetOperations.TypedTuple<Object>> initTrackingSet = new HashSet<>();
        initTrackingSet.add(new DefaultTypedTuple<>(shouldNotStarved, (double) curTime.toEpochMilli()));
        initTrackingSet.add(new DefaultTypedTuple<>(shouldNotStarved1, (double) curTime.toEpochMilli()));
        initTrackingSet.add(new DefaultTypedTuple<>(shouldStarved, (double) curTime.toEpochMilli()));
        initTrackingSet.add(new DefaultTypedTuple<>(shouldStarved1, (double) curTime.toEpochMilli()));
        initTrackingSet.add(new DefaultTypedTuple<>(shouldStarved2, (double) curTime.toEpochMilli()));
        sider.opsForZSet().add(MAIN_TRACKING_SET, initTrackingSet);
    }

    @AfterClass(groups = "functional")
    public void clearSet() {
        System.out.println("Test ran on Redis zset " + MAIN_TRACKING_SET);
        sider.delete(MAIN_TRACKING_SET);
    }

    @Test(groups = "functional", dataProvider = "timeTrackingProvider", retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testTrackingSetup(String tenantId, boolean shouldForceStarved) {
        // test detectStarvation method add tenant->tenantFirstWaitTime
        SchedulingResult result = new SchedulingResult(null, null, null, new HashSet<>(Collections.singletonList(tenantId)));
        cdlJobService.detectStarvation(result);
        Double waitTime = sider.opsForZSet().score(MAIN_TRACKING_SET, tenantId);
        Assert.assertNotNull(waitTime);
        Assert.assertEquals(waitTime, (double) curTime.toEpochMilli(), "current tenant " + tenantId);
        if (shouldForceStarved) {
            sider.opsForZSet().add(MAIN_TRACKING_SET, tenantId, (double) curTime.minus(3, ChronoUnit.DAYS).toEpochMilli());
        }
    }

    @Test(groups = "functional", dataProvider = "timeTrackingProvider", dependsOnMethods = {"testTrackingSetup"}, retryAnalyzer = SimpleRetryAnalyzer.class)
    public void testDetectStarvationWithStarved(String tenantId, boolean tenantShoudStarved) {
        SchedulingResult result = new SchedulingResult(null, null, null, Collections.emptySet());
        Set<?> starvedTenants = cdlJobService.detectStarvation(result);
        if (tenantShoudStarved) {
            Assert.assertTrue(starvedTenants.contains(tenantId));
        } else {
            Assert.assertFalse(starvedTenants.contains(tenantId));
        }
    }

    @DataProvider(name = "timeTrackingProvider")
    public Object[][] timeTrackingProvider() {
        return new Object[][]{
                {shouldNotStarved, false},
                {shouldNotStarved1, false},
                {shouldStarved, true},
                {shouldStarved1, true},
                {shouldStarved2, true},
        };
    }
}
