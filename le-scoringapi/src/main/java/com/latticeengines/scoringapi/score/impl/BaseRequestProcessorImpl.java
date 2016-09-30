package com.latticeengines.scoringapi.score.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.match.Matcher;

public class BaseRequestProcessorImpl {

    private static final Log log = LogFactory.getLog(BaseRequestProcessorImpl.class);

    @Autowired
    protected HttpStopWatch httpStopWatch;

    @Autowired
    protected List<Matcher> matchers;

    @Autowired
    protected RequestInfo requestInfo;

    protected DateTimeFormatter timestampFormatter = ISODateTimeFormat.dateTime();

    public BaseRequestProcessorImpl() {
        super();
    }

    protected void split(String key) {
        httpStopWatch.split(key);
        if (log.isInfoEnabled()) {
            log.info(key);
        }
    }

    protected Map<String, Object> extractMap(Map<String, Map<String, Object>> matchedRecordEnrichmentMap, String key) {
        Map<String, Object> map = new HashMap<>();
        if (matchedRecordEnrichmentMap.get(key) != null) {
            Map<String, Object> dataMap = matchedRecordEnrichmentMap.get(key);
            if (dataMap != null) {
                map = dataMap;
            }
        }
        return map;
    }

    protected Matcher getMatcher(boolean isBulk) {
        for (Matcher matcher : matchers) {
            if (matcher.accept(isBulk)) {
                return matcher;
            }
        }
        return null;
    }
}