package com.latticeengines.common.exposed.rest.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.lang.time.StopWatch;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.common.exposed.rest.HttpStopWatch;

@Component("httStopWatch")
public class HttpStopWatchImpl implements HttpStopWatch {

    private static final String STOPWATCH_KEY = "com.latticeengines.stopwatch";
    private static final String SPLITMAP_KEY = "com.latticeengines.stopwatch.splitmap";
    private static final String STOPWATCH_LASTSPLITTIME_KEY = "com.latticeengines.stopwatch.lastsplittime";

    public String getLogStatement(String key) {
        return String.format("{\"%sDurationMS\":\"%d\"}", key, splitAndGetTimeSinceLastSplit());
    }

    @Override
    public long split(String key) {
        long split = splitAndGetTimeSinceLastSplit();
        getOrCreateSplitMap().put(key, split);
        return split;
    }

    @Override
    public Map<String, String> getSplits() {
        Map<String, String> splitsAsStrings = new LinkedHashMap<>();
        for (String key : getOrCreateSplitMap().keySet()) {
            splitsAsStrings.put(key + "DurationMS", String.valueOf(getOrCreateSplitMap().get(key)));
        }
        splitsAsStrings.put("requestDurationMS", String.valueOf(getTime()));

        return splitsAsStrings;
    }

    @Override
    public void start() {
        getOrCreateStopWatch().start();
    }

    public long splitAndGetTimeSinceLastSplit() {
        StopWatch stopWatch = getOrCreateStopWatch();
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
        getOrCreateStopWatch().stop();
    }

    @Override
    public long getTime() {
        return getOrCreateStopWatch().getTime();
    }

    private StopWatch getOrCreateStopWatch() {
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

    @SuppressWarnings("unchecked")
    private Map<String, Long> getOrCreateSplitMap() {
        Map<String, Long> splitMap = null;
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(SPLITMAP_KEY);
        if (attribute == null) {
            splitMap = new LinkedHashMap<String, Long>();   // order the keys
            attributes.getRequest().setAttribute(SPLITMAP_KEY, splitMap);
        } else {
            splitMap = (Map<String, Long>) attribute;
        }

        return splitMap;
    }

}
