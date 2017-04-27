package com.latticeengines.datacloud.match.exposed.service;

public interface MatchMonitorService {
    void precheck(String matchVersion);

    void monitor();
}
