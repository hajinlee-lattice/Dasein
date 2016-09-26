package com.latticeengines.scoringapi.exposed.context;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class SingleRecordMeasurement extends BaseMeasurement<ScoreRequestMetrics, ScoreRequestMetrics> implements
        Measurement<ScoreRequestMetrics, ScoreRequestMetrics> {

    private ScoreRequestMetrics requestMetrics;

    public SingleRecordMeasurement(ScoreRequestMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public ScoreRequestMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public ScoreRequestMetrics getFact() {
        return requestMetrics;
    }

}
