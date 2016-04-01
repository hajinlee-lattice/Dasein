package com.latticeengines.monitor.metric.service;

import java.util.Collection;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

public interface MetricWriter {

    <F extends Fact, D extends Dimension> void write(MetricDB db, Collection<? extends Measurement<F, D>> measurements);

    void disable();

}
