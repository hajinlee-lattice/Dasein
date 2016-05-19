package com.latticeengines.scoringapi.exposed.context;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class SingleRecordMeasurement extends BaseMeasurement<RequestMetrics, RequestMetrics>
implements Measurement<RequestMetrics, RequestMetrics> {

    private RequestMetrics requestMetrics;

    public SingleRecordMeasurement(RequestMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public RequestMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public RequestMetrics getFact() {
        return requestMetrics;
    }

}
