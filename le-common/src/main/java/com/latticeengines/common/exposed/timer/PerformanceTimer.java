package com.latticeengines.common.exposed.timer;

import java.io.Closeable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class PerformanceTimer implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(PerformanceTimer.class);

    private Date start;
    private String timerMessage;
    @SuppressFBWarnings("SLF4J_LOGGER_SHOULD_BE_FINAL")
    private Logger logger;
    private long threshold = 500L;

    public PerformanceTimer() {
        this.start = new Date();
        this.logger = log;
    }

    public PerformanceTimer(Logger logger) {
        this.start = new Date();
        this.logger = logger;
    }

    public PerformanceTimer(String timerMessage) {
        this.start = new Date();
        this.timerMessage = timerMessage;
        this.logger = log;
    }

    public PerformanceTimer(String timerMessage, Logger logger) {
        this.start = new Date();
        this.timerMessage = timerMessage;
        this.logger = logger;
    }

    public void setTimerMessage(String timerMessage) {
        this.timerMessage = timerMessage;
    }

    public void setThreshold(long threshold) {
        this.threshold = threshold;
    }

    @Override
    public void close() {
        if (System.currentTimeMillis() - start.getTime() > threshold) {
            logger.info(String.format("[Metric] %s ElapsedTime=%d ms", timerMessage,
                    System.currentTimeMillis() - start.getTime()));
        }
    }
}
