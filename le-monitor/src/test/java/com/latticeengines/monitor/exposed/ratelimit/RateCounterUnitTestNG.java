package com.latticeengines.monitor.exposed.ratelimit;

import javax.servlet.http.HttpServletRequest;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

public class RateCounterUnitTestNG {
    private class RateCounterToTest extends RateCounter {
        @Override
        protected int getRatelimit() {
            return 100;
        }

        @Override
        public int getRecordCount(Object[] args) {
            return args.length;
        }
    }

    private RateCounterToTest rateCounter;
    private HttpServletRequest request;

    @BeforeTest(groups = "unit")
    public void setup() {
        request = Mockito.mock(HttpServletRequest.class);
        rateCounter = new RateCounterToTest();
    }

    @Test(groups = "unit")
    public void testShouldAccept() {
        int numRecords = rateCounter.getRecordCount(new Object[10]);
        boolean noException = false;
        try {
            rateCounter.shouldAccept(request, numRecords);
            rateCounter.shouldAccept(request, numRecords);
            rateCounter.shouldAccept(request, numRecords);
            rateCounter.shouldAccept(request, numRecords);
            rateCounter.shouldAccept(request, numRecords);
            noException = true;
        } catch (RateLimitException exc) {
            Assert.fail();
        }
        Assert.assertTrue(noException);
    }

    @Test(groups = "unit", expectedExceptions = {RateLimitException.class}, dependsOnMethods = "testShouldAccept")
    public void testShouldNotAccept() {
        int numRecords = rateCounter.getRecordCount(new Object[40]);
        rateCounter.shouldAccept(request, numRecords);
        rateCounter.shouldAccept(request, numRecords);
        rateCounter.shouldAccept(request, numRecords);
    }

    @Test(groups = "unit", dependsOnMethods = "testShouldNotAccept")
    public void testDecrementCounter() {
        int numRepeats = 20;
        boolean noException = false;
        try {
            for (int i = 0; i < numRepeats; i++) {
                rateCounter.decrementCounter(10);
            }
            noException = true;
        } catch (Exception exc) {
            Assert.fail();
        }
        Assert.assertTrue(noException);
    }

    @Test(groups = "unit")
    public void getHttpRequest() {
        Object[] args = new Object[] { "abc", 123, new String[] { "xyz", "opq" }, request, null, 34.5, 99L};
        Assert.assertNotNull(rateCounter.getHttpRequest(args));
    }

    @Test(groups = "unit")
    public void getHttpRequestAsNull() {
        Object[] args = new Object[] { "abc", 123, new String[] { "xyz", "opq" }, 'a', null, 34.5, 99L};
        Assert.assertNull(rateCounter.getHttpRequest(args));
    }
}
