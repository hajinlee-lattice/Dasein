package com.latticeengines.domain.exposed.monitor.metric;

import java.util.Collection;
import java.util.Collections;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.MetricStore;
import com.latticeengines.common.exposed.metric.RetentionPolicy;

public abstract class BaseMeasurement<F extends Fact, D extends Dimension> implements Measurement<F, D> {
    public abstract D getDimension();

    public abstract F getFact();

    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.DEFAULT;
    }

    public Collection<MetricStore> getMetricStores() {
        return Collections.<MetricStore> singleton(MetricStoreImpl.INFLUX_DB);
    }
}
