package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class UlyssesAccMeasurement extends BaseMeasurement<UlyssesAccMetrics, UlyssesAccMetrics>
        implements Measurement<UlyssesAccMetrics, UlyssesAccMetrics> {

    private UlyssesAccMetrics requestMetrics;

    public UlyssesAccMeasurement(UlyssesAccMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public UlyssesAccMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public UlyssesAccMetrics getFact() {
        return requestMetrics;
    }

}
