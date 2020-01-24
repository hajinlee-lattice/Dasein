package com.latticeengines.common.exposed.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SleepUtils {

    protected SleepUtils() {
        throw new UnsupportedOperationException();
    }

    private static final Logger log = LoggerFactory.getLogger(SleepUtils.class);

    public static void sleep(long mills) {
        try {
            Thread.sleep(mills);
        } catch (InterruptedException e) {
            log.warn("Sleep is interrupted", e);
        }
    }

}
