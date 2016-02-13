package com.latticeengines.monitor.exposed.metric.service;

import java.util.Collection;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

public interface MetricService {

    <F extends Fact, D extends Dimension> void write(MetricDB db, Measurement<F, D> measurement);

    <F extends Fact, D extends Dimension> void write(MetricDB db, Collection<? extends Measurement<F, D>> measurements);

    void disable();

}
