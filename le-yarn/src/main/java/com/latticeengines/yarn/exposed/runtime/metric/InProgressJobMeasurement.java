package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.metric.RetentionPolicy;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;
import com.latticeengines.domain.exposed.monitor.metric.RetentionPolicyImpl;

public class InProgressJobMeasurement extends BaseMeasurement<InProgressJobMetric, InProgressJobMetric> implements
        Measurement<InProgressJobMetric, InProgressJobMetric> {

    private InProgressJobMetric jobMetric;

    public InProgressJobMeasurement(InProgressJobMetric jobMetric) {
        this.jobMetric = jobMetric;
    }

    @Override
    public RetentionPolicy getRetentionPolicy() {
        return RetentionPolicyImpl.TWO_QUARTERS;
    }

    @Override
    public InProgressJobMetric getDimension() {
        return jobMetric;
    }

    @Override
    public InProgressJobMetric getFact() {
        return jobMetric;
    }

}
