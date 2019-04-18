package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class UlyssesAccSegmentMeasurement extends BaseMeasurement<UlyssesAccSegmentMetrics, UlyssesAccSegmentMetrics>
        implements Measurement<UlyssesAccSegmentMetrics, UlyssesAccSegmentMetrics> {

    private UlyssesAccSegmentMetrics requestMetrics;

    public UlyssesAccSegmentMeasurement(UlyssesAccSegmentMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public UlyssesAccSegmentMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public UlyssesAccSegmentMetrics getFact() {
        return requestMetrics;
    }
}
