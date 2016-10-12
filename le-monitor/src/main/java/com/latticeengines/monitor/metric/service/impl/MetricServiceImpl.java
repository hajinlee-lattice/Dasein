package com.latticeengines.monitor.metric.service.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.monitor.metric.service.MetricWriter;

@Component("metricService")
public class MetricServiceImpl implements MetricService {

    @Autowired
    private List<MetricWriter> metricWriters;

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db, Measurement<F, D> measurement) {
        write(db, Collections.singletonList(measurement), null);
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db, Measurement<F, D> measurement,
            Map<String, Object> fieldMap) {
        write(db, Collections.singletonList(measurement), Collections.singletonList(fieldMap));
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db, List<? extends Measurement<F, D>> measurements) {
        write(db, measurements, null);
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db,
            List<? extends Measurement<F, D>> measurements, List<Map<String, Object>> fieldMaps) {
        for (MetricWriter writer : metricWriters) {
            writer.write(db, measurements, fieldMaps);
        }
    }

    @Override
    public void disable() {
        for (MetricWriter writer : metricWriters) {
            writer.disable();
        }
    }

}
