package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class UlyssesPurchaseHistMeasurement
        extends BaseMeasurement<UlyssesPurchaseHistMetrics, UlyssesPurchaseHistMetrics>
        implements Measurement<UlyssesPurchaseHistMetrics, UlyssesPurchaseHistMetrics> {

    private UlyssesPurchaseHistMetrics requestMetrics;

    public UlyssesPurchaseHistMeasurement(UlyssesPurchaseHistMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public UlyssesPurchaseHistMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public UlyssesPurchaseHistMetrics getFact() {
        return requestMetrics;
    }
}
