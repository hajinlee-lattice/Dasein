package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class UlyssesTalkingPointsMeasurement
        extends BaseMeasurement<UlyssesTalkingPointsMetrics, UlyssesTalkingPointsMetrics>
        implements Measurement<UlyssesTalkingPointsMetrics, UlyssesTalkingPointsMetrics> {

    private UlyssesTalkingPointsMetrics requestMetrics;

    public UlyssesTalkingPointsMeasurement(UlyssesTalkingPointsMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public UlyssesTalkingPointsMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public UlyssesTalkingPointsMetrics getFact() {
        return requestMetrics;
    }
}
