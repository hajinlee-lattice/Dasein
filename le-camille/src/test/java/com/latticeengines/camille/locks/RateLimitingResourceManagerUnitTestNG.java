package com.latticeengines.camille.locks;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.locks.RateLimitedResourceManager;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.domain.exposed.camille.locks.RateLimitDefinition;
import com.latticeengines.domain.exposed.camille.locks.RateLimitedAcquisition;

public class RateLimitingResourceManagerUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(RateLimitingResourceManagerUnitTestNG.class);

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit", retryAnalyzer = SimpleRetryPolicy.class)
    public void testHappyCase() throws Exception {
        String resource = "HappyTest";
        RateLimitDefinition definition = RateLimitDefinition.divisionPrivateDefinition();
        definition.addQuota("Counter1", new RateLimitDefinition.Quota(10, 3, TimeUnit.SECONDS));
        RateLimitedResourceManager.registerResource(resource, definition);

        Map<String, Long> inquiringQuantities = new HashMap<>();
        inquiringQuantities.put("Counter1", 1L);
        for (int i = 0; i < 10; i++) {
            RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10,
                    TimeUnit.MICROSECONDS);
            Assert.assertTrue(acquisition.isAllowed(), "Acquisition should be allowed.");
        }

        RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10,
                TimeUnit.MICROSECONDS);
        Assert.assertFalse(acquisition.isAllowed(), "Acquisition should be rejected.");
        Assert.assertTrue(acquisition.getExceedingQuotas().contains("Counter1_10_3_SECONDS"));

        Thread.sleep(3000L);

        acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10, TimeUnit.MICROSECONDS);
        Assert.assertTrue(acquisition.isAllowed(), "Acquisition should be allowed.");
    }

    @Test(groups = "unit", retryAnalyzer = SimpleRetryPolicy.class)
    public void testLocalStore() throws Exception {
        String resource = "LocalStoreTest";
        RateLimitDefinition definition = RateLimitDefinition.divisionPrivateDefinition();
        definition.addQuota("Counter1", new RateLimitDefinition.Quota(10, 3, TimeUnit.SECONDS));
        RateLimitedResourceManager.registerResource(resource, definition);

        Map<String, Long> inquiringQuantities = new HashMap<>();
        inquiringQuantities.put("Counter1", 1L);

        for (int i = 0; i < 10; i++) {
            RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10,
                    TimeUnit.MICROSECONDS);
            Assert.assertTrue(acquisition.isAllowed(), "Acquisition should be allowed in zk.");
        }

        RateLimitedAcquisition acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10,
                TimeUnit.MICROSECONDS);
        Assert.assertFalse(acquisition.isAllowed(), "Acquisition should be rejected by zk.");
        Assert.assertTrue(acquisition.getExceedingQuotas().contains("Counter1_10_3_SECONDS"));

        CamilleTestEnvironment.stop();

        for (int i = 0; i < 10; i++) {
            acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10, TimeUnit.MICROSECONDS);
            Assert.assertTrue(acquisition.isAllowed(), "Acquisition should be allowed in local store.");
        }

        Thread.sleep(3000L);
        CamilleTestEnvironment.start();
        acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10, TimeUnit.MICROSECONDS);
        Assert.assertTrue(acquisition.isAllowed(), "Acquisition should be allowed with zk.");

        Thread.sleep(3000L);
        CamilleTestEnvironment.stop();
        acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 10, TimeUnit.MICROSECONDS);
        Assert.assertTrue(acquisition.isAllowed(), "Acquisition should be allowed with local store.");
    }

    @Test(groups = "unit", retryAnalyzer = SimpleRetryPolicy.class)
    public void testLimitingCorrectnessZK() throws Exception {
        testLimitingCorrectness(false);
    }

    @Test(groups = "unit", dependsOnMethods = "testLimitingCorrectnessZK", retryAnalyzer = SimpleRetryPolicy.class)
    public void testLimitingCorrectnessLocal() throws Exception {
        testLimitingCorrectness(true);
    }

    private void testLimitingCorrectness(boolean localMode) throws Exception {
        String resource = "LimitingCorrectness";
        int numThreads = 16;
        int numLoops = 100;

        RateLimitDefinition definition = RateLimitDefinition.divisionPrivateDefinition();
        RateLimitDefinition.Quota quota1 = new RateLimitDefinition.Quota(100, 1, TimeUnit.SECONDS);
        RateLimitDefinition.Quota quota2 = new RateLimitDefinition.Quota(200, 4, TimeUnit.SECONDS);
        definition.addQuota("Counter1", quota1);
        definition.addQuota("Counter2", quota2);
        RateLimitedResourceManager.registerResource(resource, definition);

        if (localMode) {
            CamilleTestEnvironment.stop();
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        ConcurrentLinkedDeque<Long> cntr1Timestamps = new ConcurrentLinkedDeque<>();
        ConcurrentLinkedDeque<Long> cntr2Timestamps = new ConcurrentLinkedDeque<>();
        ConcurrentMap<String, Integer> distribution = new ConcurrentHashMap<>();

        Map<String, Long> inquiringQuantities = new HashMap<>();
        inquiringQuantities.put("Counter1", 1L);
        inquiringQuantities.put("Counter2", 3L);

        for (int i = 0; i < numThreads; i++) {
            executor.execute(() -> {
                RateLimitedAcquisition acquisition;
                for (int j = 0; j < numLoops; j++) {
                    try {
                        Thread.sleep(new Random().nextInt(100));
                    } catch (Exception e) {
                        // ignore
                    }
                    acquisition = RateLimitedResourceManager.acquire(resource, inquiringQuantities, 1,
                            TimeUnit.SECONDS);
                    if (acquisition.isAllowed()) {
                        cntr1Timestamps.add(acquisition.getAcquiredTimestamp());
                        cntr2Timestamps.add(acquisition.getAcquiredTimestamp());
                        String threadName = Thread.currentThread().getName();
                        distribution.put(threadName, distribution.getOrDefault(threadName, 0) + 1);
                    }
                }
            });
        }

        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.MINUTES);

        List<Long> cntr1TimestampsSorted = new ArrayList<>(cntr1Timestamps);
        List<Long> cntr2TimestampsSorted = new ArrayList<>(cntr2Timestamps);
        Collections.sort(cntr1TimestampsSorted);
        Collections.sort(cntr2TimestampsSorted);

        double tolerant = localMode ? 2.0 : 1.3;
        verifyTimestamps(cntr1TimestampsSorted, 1, quota1, tolerant);
        verifyTimestamps(cntr2TimestampsSorted, 3, quota2, tolerant);
        verifyUniformity(distribution, numThreads);

        if (localMode) {
            CamilleTestEnvironment.start();
        }
    }

    private void verifyTimestamps(List<Long> ts, long increment, RateLimitDefinition.Quota quota, double tolerant) {
        int p2 = 0;
        long window = quota.getTimeUnit().toMillis(quota.getDuration());
        long max = quota.getMaxQuantity();
        for (int p1 = 0; p1 < ts.size(); p1++) {
            while (p2 < ts.size() - 1 && ts.get(p2 + 1) - ts.get(p1) <= window) {
                p2++;
            }
            if ((p2 - p1 + 1) * increment > max) {
                log.warn(String.format("The quota limit %d was exceeded (%d) between %d and %d (%d:%d)", max,
                        (p2 - p1 + 1) * increment, ts.get(p1), ts.get(p2), p2 - p1 + 1, ts.get(p2) - ts.get(p1)));
            }

            Assert.assertTrue((p2 - p1 + 1) * increment <= tolerant * max,
                    String.format("The quota limit %d was exceeded by more than 50 %% (%d) between %d and %d (%d:%d)",
                            max, (p2 - p1 + 1) * increment, ts.get(p1), ts.get(p2), p2 - p1 + 1,
                            ts.get(p2) - ts.get(p1)));
            if (p2 == ts.size() - 1) {
                break;
            }
        }
    }

    private void verifyUniformity(Map<String, Integer> distribution, int numThreads) {
        Assert.assertEquals(distribution.size(), numThreads);
        double[] doubles = new double[distribution.size()];
        int i = 0;
        for (Integer value : distribution.values()) {
            doubles[i] = value;
            i++;
        }
        double std = new StandardDeviation().evaluate(doubles);
        double mean = new Mean().evaluate(doubles);
        log.info(String.format("Mean count = %.2f; Standard deviation = %.2f [%.2f%%]", mean, std, 100 * std / mean));
        Assert.assertTrue(std / mean < 0.40, "Standard deviation should be smaller than 40%");
    }
}
