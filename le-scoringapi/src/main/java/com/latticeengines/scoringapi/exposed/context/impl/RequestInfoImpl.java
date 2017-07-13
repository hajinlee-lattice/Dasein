package com.latticeengines.scoringapi.exposed.context.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.google.common.base.Strings;
import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;

@Component("requestInfo")
public class RequestInfoImpl implements RequestInfo {

    private static final Logger log = LoggerFactory.getLogger(RequestInfoImpl.class);
    private static final String CONTEXT_KEY = "com.latticeengines.scoringapi.requestinfo";

    @Autowired
    private HttpStopWatch httpStopWatch;

    @Override
    public String get(String key) {
        Map<String, String> context = getOrCreate();
        return Strings.nullToEmpty(context.get(key));
    }

    @Override
    public void put(String key, String value) {
        Map<String, String> context = getOrCreate();
        context.put(key, value);
    }

    @Override
    public void putAll(Map<String, String> map) {
        Map<String, String> context = getOrCreate();
        context.putAll(map);
    }

    @Override
    public void logSummary(Map<String, String> stopWatchSplits) {
        Map<String, String> context = getOrCreate();
        putAll(stopWatchSplits);
        log.info(JsonUtils.serialize(context));
    }

    @Override
    public Map<String, String> getStopWatchSplits() {
        return httpStopWatch.getSplits();
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> getOrCreate() {
        Map<String, String> context = null;
        ServletRequestAttributes attributes = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
        Object attribute = attributes.getRequest().getAttribute(CONTEXT_KEY);
        if (attribute == null) {
            context = new LinkedHashMap<>();
            attributes.getRequest().setAttribute(CONTEXT_KEY, context);
        } else {
            context = (Map<String, String>) attribute;
        }
        return context;
    }

    @Override
    public void remove(String key) {
        Map<String, String> context = getOrCreate();
        context.remove(key);
    }

    @Override
    public void logAggregateSummary(Map<String, String> aggregateDurationStopWatchSplits) {
        Map<String, String> context = getOrCreate();
        putAll(aggregateDurationStopWatchSplits);
        log.info(JsonUtils.serialize(context));
    }

}
