package com.latticeengines.scoringapi.exposed.context;

import java.util.Map;

public interface RequestInfo {

    static final String TENANT = "Tenant";

    String get(String key);

    void put(String key, String value);

    void putAll(Map<String, String> map);

    void logSummary();
}
