package com.latticeengines.scoringapi.exposed.context.impl;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;

@Component("requestInfo")
public class RequestInfoImpl implements RequestInfo {

    private static final Log log = LogFactory.getLog(RequestInfoImpl.class);
    private static final String CONTEXT_KEY = "com.latticeengines.scoringapi.requestinfo";

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
    public void logSummary() {
        Map<String, String> context = getOrCreate();
        log.info(JsonUtils.serialize(context));
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


}
