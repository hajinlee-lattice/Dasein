package com.latticeengines.scoringapi.exposed.context;

import java.util.Map;

public interface RequestInfo {

    String TENANT = "Tenant";

    String get(String key);

    void put(String key, String value);

    void putAll(Map<String, String> map);

    void logSummary(Map<String, String> stopWatchSplits);

    Map<String, String> getStopWatchSplits();

    void remove(String key);

    void logAggregateSummary(Map<String, String> aggregateDurationStopWatchSplits);
}
