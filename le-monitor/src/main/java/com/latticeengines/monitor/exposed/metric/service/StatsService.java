package com.latticeengines.monitor.exposed.metric.service;

import com.latticeengines.monitor.exposed.metric.stats.Inspection;

public interface StatsService {

    void register(Inspection inspection);

}
