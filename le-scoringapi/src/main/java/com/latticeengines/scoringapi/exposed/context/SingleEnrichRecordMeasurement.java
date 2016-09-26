package com.latticeengines.scoringapi.exposed.context;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class SingleEnrichRecordMeasurement extends BaseMeasurement<EnrichRequestMetrics, EnrichRequestMetrics>
        implements Measurement<EnrichRequestMetrics, EnrichRequestMetrics> {

    private EnrichRequestMetrics requestMetrics;

    public SingleEnrichRecordMeasurement(EnrichRequestMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public EnrichRequestMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public EnrichRequestMetrics getFact() {
        return requestMetrics;
    }
}
