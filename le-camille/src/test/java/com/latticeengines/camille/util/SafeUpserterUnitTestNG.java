package com.latticeengines.camille.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.latticeengines.camille.exposed.config.ConfigurationController;
import com.latticeengines.camille.exposed.util.CamilleTestEnvironment;
import com.latticeengines.camille.exposed.util.DocumentUtils;
import com.latticeengines.camille.exposed.util.SafeUpserter;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.camille.scopes.CustomerSpaceScope;

public class SafeUpserterUnitTestNG {

    @BeforeMethod(groups = "unit")
    public void setUp() throws Exception {
        CamilleTestEnvironment.start();
    }

    @AfterMethod(groups = "unit")
    public void tearDown() throws Exception {
        CamilleTestEnvironment.stop();
    }

    private static class TestDocument {
        public int test;
    }

    @Test(groups = "unit")
    public void testSafeUpserterParameterCorrectness() {
        CustomerSpaceScope scope = new CustomerSpaceScope(CamilleTestEnvironment.getCustomerSpace());
        SafeUpserter upserter = new SafeUpserter();
        upserter.upsert(scope, new Path("/test"), existing -> {
            Assert.assertNull(existing);
            return new TestDocument();
        }, TestDocument.class);

        upserter.upsert(scope, new Path("/test"), existing -> {
            Assert.assertNotNull(existing);
            return new TestDocument();
        }, TestDocument.class);
    }

    /**
     * Should be able to get all the way to test = 5. Specifically, the
     * SafeUpserter should act like a (terrible) lock around the increment of
     * TestDocument.test.
     */
    @Test(groups = "unit")
    public void testSafeUpserterSafety() throws Exception {
        final CustomerSpaceScope scope = new CustomerSpaceScope(CamilleTestEnvironment.getCustomerSpace());
        final Path path = new Path("/test");
        final int max = 5;

        ExecutorService pool = Executors.newFixedThreadPool(10);
        try {
            for (int i = 0; i < 5; ++i) {
                pool.submit(() -> {
                    SafeUpserter upserter = new SafeUpserter((int) (Math.random() * 100.0), 100);
                    upserter.upsert(scope, path, existing -> {
                        TestDocument toReturn = new TestDocument();
                        if (existing == null) {
                            toReturn.test = 1;
                        } else {
                            toReturn.test = existing.test + 1;
                        }

                        return toReturn;
                    }, TestDocument.class);
                });
            }
        } finally {
            pool.shutdown();
            pool.awaitTermination(1, TimeUnit.MINUTES);
        }

        ConfigurationController<CustomerSpaceScope> controller = ConfigurationController.construct(scope);
        TestDocument doc = DocumentUtils.toTypesafeDocument(controller.get(path), TestDocument.class);
        Assert.assertEquals(doc.test, max);
    }
}
