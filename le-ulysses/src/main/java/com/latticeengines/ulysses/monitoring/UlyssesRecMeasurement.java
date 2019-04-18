package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class UlyssesRecMeasurement extends BaseMeasurement<UlyssesRecMetrics, UlyssesRecMetrics>
        implements Measurement<UlyssesRecMetrics, UlyssesRecMetrics> {

    private UlyssesRecMetrics requestMetrics;

    public UlyssesRecMeasurement(UlyssesRecMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public UlyssesRecMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public UlyssesRecMetrics getFact() {
        return requestMetrics;
    }
}
