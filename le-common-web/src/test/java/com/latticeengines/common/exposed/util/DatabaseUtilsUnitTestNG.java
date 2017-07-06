package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertNotNull;

import org.apache.commons.collections.Closure;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.LockAcquisitionException;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DatabaseUtilsUnitTestNG {
    private Integer counter = new Integer(0);

    @Test(groups = "unit")
    public void testRetryFailure() {
        Exception thrown = null;
        int startCounterValue = counter;
        int maxRetry = 8;
        try {
            DatabaseUtils.retry("failure", maxRetry, new Closure() {
                @Override
                public void execute(Object input) {
                    counter++;
                    throw new LockAcquisitionException("Deadlock!", null);
                }
            });
        } catch (Exception e) {
            thrown = e;
        }
        assertNotNull(thrown);
        Assert.assertEquals(counter.intValue(), startCounterValue + maxRetry);
    }

    @Test(groups = "unit", dependsOnMethods = { "testRetryFailure" })
    public void testRetryCustomFailure() {
        Exception thrown = null;
        int startCounterValue = counter;
        int maxRetry = 4;
        try {
            DatabaseUtils.retry("customFailure", maxRetry, ConstraintViolationException.class,
                    "Ignore unique key violation for UQ__SELECTED__1234", "UQ__SELECTED__", new Closure() {
                        @Override
                        public void execute(Object input) {
                            counter++;
                            throw new ConstraintViolationException("UQ__SELECTED__1234 violation", null,
                                    "UQ__SELECTED__1234");
                        }
                    });
        } catch (Exception e) {
            thrown = e;
        }
        assertNotNull(thrown);
        Assert.assertEquals(counter.intValue(), startCounterValue + maxRetry);
    }

    @Test(groups = "unit")
    public void testRetrySuccess() {
        DatabaseUtils.retry("success", new Closure() {

            @Override
            public void execute(Object input) {
            }
        });
    }
}
