package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class UlyssesAttributeMeasurement extends BaseMeasurement<UlyssesAttributeMetrics, UlyssesAttributeMetrics>
        implements Measurement<UlyssesAttributeMetrics, UlyssesAttributeMetrics> {

    private UlyssesAttributeMetrics requestMetrics;

    public UlyssesAttributeMeasurement(UlyssesAttributeMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public UlyssesAttributeMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public UlyssesAttributeMetrics getFact() {
        return requestMetrics;
    }
}
