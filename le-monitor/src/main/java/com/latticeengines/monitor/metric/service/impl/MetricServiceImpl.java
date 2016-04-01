package com.latticeengines.monitor.metric.service.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
    public <F extends Fact, D extends Dimension> void
    write(MetricDB db, Measurement<F, D> measurement) {
        write(db, Collections.singleton(measurement));
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db,
            Collection<? extends Measurement<F, D>> measurements) {
        for (MetricWriter writer: metricWriters) {
            writer.write(db, measurements);
        }
    }

    @Override
    public void disable() {
        for (MetricWriter writer: metricWriters) {
            writer.disable();
        }
    }

}
