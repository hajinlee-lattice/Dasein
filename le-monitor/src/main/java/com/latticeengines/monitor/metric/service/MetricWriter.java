package com.latticeengines.monitor.metric.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

public interface MetricWriter {

    <F extends Fact, D extends Dimension> void write(MetricDB db, List<? extends Measurement<F, D>> measurements,
            List<Map<String, Object>> fieldMaps);

    void disable();

    void enable();

}
