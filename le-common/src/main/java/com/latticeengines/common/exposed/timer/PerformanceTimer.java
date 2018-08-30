package com.latticeengines.common.exposed.timer;

import java.io.Closeable;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PerformanceTimer implements Closeable {
    private static final Logger log = LoggerFactory.getLogger(PerformanceTimer.class);

    private Date start;
    private String timerMessage;
    private Logger logger;
    private static final long TIME_THRESHOLD = 500L;

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

    @Override
    public void close() {
        if (new Date().getTime() - start.getTime() > TIME_THRESHOLD) {
            logger.info(String.format("[Metric] %s ElapsedTime=%d ms", timerMessage,
                    new Date().getTime() - start.getTime()));
        }
    }
}
