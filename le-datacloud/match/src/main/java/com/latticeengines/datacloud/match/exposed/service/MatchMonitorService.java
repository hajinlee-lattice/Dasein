package com.latticeengines.datacloud.match.exposed.service;

public interface MatchMonitorService {
    void monitor();

    void pushMetrics(String service, String message);
}
