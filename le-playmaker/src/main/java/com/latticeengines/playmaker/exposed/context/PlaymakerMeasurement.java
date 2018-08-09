package com.latticeengines.playmaker.exposed.context;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class PlaymakerMeasurement extends BaseMeasurement<PlaymakerMetrics, PlaymakerMetrics>
        implements Measurement<PlaymakerMetrics, PlaymakerMetrics> {

    private PlaymakerMetrics requestMetrics;

    public PlaymakerMeasurement(PlaymakerMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public PlaymakerMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public PlaymakerMetrics getFact() {
        return requestMetrics;
    }

}
