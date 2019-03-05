package com.latticeengines.scoringapi.score.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.rest.HttpStopWatch;
import com.latticeengines.scoringapi.exposed.context.RequestInfo;
import com.latticeengines.scoringapi.match.Matcher;

public class BaseRequestProcessorImpl {

    private static final Logger log = LoggerFactory.getLogger(BaseRequestProcessorImpl.class);

    @Inject
    protected HttpStopWatch httpStopWatch;

    @Inject
    protected List<Matcher> matchers;

    @Inject
    protected RequestInfo requestInfo;

    protected DateTimeFormatter timestampFormatter = ISODateTimeFormat.dateTime();

    public BaseRequestProcessorImpl() {
        super();
    }

    protected void split(String key) {
        httpStopWatch.split(key);
        if (log.isDebugEnabled()) {
            log.debug(key);
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
