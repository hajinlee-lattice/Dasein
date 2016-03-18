package com.latticeengines.common.exposed.rest.impl;

import org.apache.commons.lang.time.StopWatch;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.common.exposed.rest.HttpStopWatch;

@Component("httStopWatch")
public class HttpStopWatchImpl implements HttpStopWatch {

    private static final String STOPWATCH_KEY = "com.latticeengines.stopwatch";
    private static final String STOPWATCH_LASTSPLITTIME_KEY = "com.latticeengines.stopwatch.lastsplittime";

    @Override
    public String getLogStatement(String key) {
        return String.format("{\"%sDurationMS\":\"%d\"}", key, splitAndGetTimeSinceLastSplit());
    }

    @Override
    public void start() {
        getOrCreate().start();
    }

    @Override
    public long splitAndGetTimeSinceLastSplit() {
        StopWatch stopWatch = getOrCreate();
        long lastSplitTime = getLastSplitTime();
        stopWatch.split();
        long splitTime = stopWatch.getSplitTime();
        setLastSplitTime(splitTime);

        return splitTime - lastSplitTime;
    }

    private long getLastSplitTime() {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(STOPWATCH_LASTSPLITTIME_KEY);
        if (attribute == null) {
            return 0;
        }
        return (Long) attribute;
    }

    private void setLastSplitTime(long splitTime) {
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        attributes.getRequest().setAttribute(STOPWATCH_LASTSPLITTIME_KEY, splitTime);
    }

    @Override
    public void stop() {
        getOrCreate().stop();
    }

    @Override
    public long getTime() {
        return getOrCreate().getTime();
    }

    private StopWatch getOrCreate() {
        StopWatch stopWatch = null;
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(STOPWATCH_KEY);
        if (attribute == null) {
            stopWatch = new StopWatch();
            attributes.getRequest().setAttribute(STOPWATCH_KEY, stopWatch);
        } else {
            stopWatch = (StopWatch) attribute;
        }

        return stopWatch;
    }

}
