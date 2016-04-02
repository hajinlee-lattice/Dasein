package com.latticeengines.scoringapi.context;

import java.util.Map;

public interface RequestInfo {

    void put(String key, String value);

    void putAll(Map<String, String> map);

    void logSummary();
}
