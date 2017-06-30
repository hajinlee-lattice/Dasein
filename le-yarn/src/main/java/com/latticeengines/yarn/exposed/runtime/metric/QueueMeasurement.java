package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class QueueMeasurement extends BaseMeasurement<QueueMetric, QueueMetric> implements
        Measurement<QueueMetric, QueueMetric> {

    private QueueMetric metric;

    public QueueMeasurement(QueueMetric metric) {
        this.metric = metric;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.TWO_QUARTERS;
    }

    @Override
    public QueueMetric getDimension() {
        return metric;
    }

    @Override
    public QueueMetric getFact() {
        return metric;
    }

}
