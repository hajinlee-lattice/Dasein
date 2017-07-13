package com.latticeengines.monitor.exposed.metrics;

import java.io.Closeable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTimer implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTimer.class);

    private Date start;
    private String methodName;

    public PerformanceTimer(String methodName) {
        this.start = new Date();
        this.methodName = methodName;
    }

    @Override
    public void close() {
        log.info(String.format("Metrics for %s ElapsedTime=%d ms", methodName, new Date().getTime() - start.getTime()));
    }
}
