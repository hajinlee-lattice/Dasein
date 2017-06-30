package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class CompletedJobMeasurement extends BaseMeasurement<CompletedJobMetric, CompletedJobMetric> implements
        Measurement<CompletedJobMetric, CompletedJobMetric> {

    private CompletedJobMetric jobMetric;

    public CompletedJobMeasurement(CompletedJobMetric jobMetric) {
        this.jobMetric = jobMetric;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.TWO_QUARTERS;
    }

    @Override
    public CompletedJobMetric getDimension() {
        return jobMetric;
    }

    @Override
    public CompletedJobMetric getFact() {
        return jobMetric;
    }

}
