package com.latticeengines.monitor.exposed.metrics;

import java.io.Closeable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTimer implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTimer.class);

    private Date start;
    private String timerMessage;

    public PerformanceTimer(String timerMessage) {
        this.start = new Date();
        this.timerMessage = timerMessage;
    }

    @Override
    public void close() {
        log.info(String.format("[Metric] %s ElapsedTime=%d ms", timerMessage, new Date().getTime() - start.getTime()));
    }
}
