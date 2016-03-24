package com.latticeengines.common.exposed.util;

import static org.testng.Assert.assertNotNull;

import org.apache.commons.collections.Closure;
import org.hibernate.exception.LockAcquisitionException;
import org.testng.annotations.Test;

public class DatabaseUtilsUnitTestNG {
    @Test(groups = "unit")
    public void testRetryFailure() {
        Exception thrown = null;
        try {
            DatabaseUtils.retry("failure", 8, new Closure() {
                @Override
                public void execute(Object input) {
                    throw new LockAcquisitionException("Deadlock!", null);
                }
            });
        } catch (Exception e) {
            thrown = e;
        }
        assertNotNull(thrown);
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
