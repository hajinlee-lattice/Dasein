package com.latticeengines.common.exposed.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.testng.annotations.Test;

public class LogContextUnitTestNG {
    @Test(groups = "unit")
    public void test() {
        int interestingValue = 42;

        // Create a log context that will last only for the duration of this
        // try-with-resources block.
        try (LogContext context = new LogContext("InterestingValue", interestingValue)) {

            // Any log message emitted within this block will have the values
            // associated with this (and any other) enclosing log contexts.
            log.info("Important information!");
        }
    }

    private static final Log log = LogFactory.getLog(LogContextUnitTestNG.class);
}
