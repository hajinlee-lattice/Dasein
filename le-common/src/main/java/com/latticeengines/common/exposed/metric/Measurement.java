package com.latticeengines.common.exposed.metric;

import java.util.Collection;

public interface Measurement<F extends Fact, D extends Dimension> {

    D getDimension();

    F getFact();

    RetentionPolicy getRetentionPolicy();

    Collection<MetricStore> getMetricStores();

}
