package com.latticeengines.camille.exposed.watchers;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;

import org.springframework.retry.support.RetryTemplate;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.common.exposed.util.RetryUtils;
import com.latticeengines.common.exposed.util.ThreadPoolUtils;

public class DebugGatewayWatcherUnitTestNG {

    @BeforeClass(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
        DebugGatewayWatcher.initialize();
    }

    @AfterClass(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    @Test(groups = "unit", enabled = false)
    public void testSimpleCheck() {
        simpleCheck("T0", "PT5S", 5000).run();
    }

    @Test(groups = "unit", enabled = false)
    public void testDebugGateway() {
        List<Runnable> runnables = new ArrayList<>();
        runnables.add(simpleCheck("T1", "PT3S", 3000));
        runnables.add(simpleCheck("T2", "PT4S", 4000));
        runnables.add(simpleCheck("T3", "PT2S", 2000));
        runnables.add(simpleCheck("T4", "PT1S", 1000));
        runnables.add(simpleCheck("T5", "PT3S", 3000));
        runnables.add(simpleCheck("T6", "PT5S", 5000));
        runnables.add(simpleCheck("T7", "PT2.5S", 2500));
        runnables.add(simpleCheck("T8", "PT3.6S", 4000));
        ExecutorService pool = ThreadPoolUtils.getFixedSizeThreadPool("test-pool", 8);
        ThreadPoolUtils.runRunnablesInParallel(pool, runnables, 1, 1);
    }

    private Runnable simpleCheck(String tenantId, String duration, long wait2) {
        return () -> {
            try {
                Thread.sleep(new Random().nextInt(10000));
                Assert.assertFalse(DebugGatewayWatcher.hasPassport(tenantId));
                DebugGatewayWatcher.applyForPassportAsync(tenantId, duration);
                RetryTemplate retry = RetryUtils.getExponentialBackoffRetryTemplate( //
                        4, 250, 1, null);
                Assert.assertTrue(retry.execute(ctx -> {
                    boolean hasPassport = DebugGatewayWatcher.hasPassport(tenantId);
                    if (!hasPassport) {
                        throw new IllegalStateException("Attempt=" + ctx.getRetryCount() + ": "
                                + tenantId + " does not have passport yet.");
                    }
                    return true;
                }));
                Thread.sleep(wait2);
                Assert.assertFalse(DebugGatewayWatcher.hasPassport(tenantId));
            } catch (Exception e) {
                Assert.fail("Encountered exception", e);
            }
        };
    }

}
