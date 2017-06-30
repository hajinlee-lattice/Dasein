package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class CompletedJobByAppIdMeasurement extends BaseMeasurement<CompletedJobByAppIdMetric, CompletedJobByAppIdMetric> implements
        Measurement<CompletedJobByAppIdMetric, CompletedJobByAppIdMetric> {

    private CompletedJobByAppIdMetric jobMetric;

    public CompletedJobByAppIdMeasurement(CompletedJobByAppIdMetric jobMetric) {
        this.jobMetric = jobMetric;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.TWO_QUARTERS;
    }

    @Override
    public CompletedJobByAppIdMetric getDimension() {
        return jobMetric;
    }

    @Override
    public CompletedJobByAppIdMetric getFact() {
        return jobMetric;
    }

}
