package com.latticeengines.playmaker.measurements;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class PlaymakerAccExtMeasurement extends BaseMeasurement<PlaymakerAccExtMetrics, PlaymakerAccExtMetrics>
        implements Measurement<PlaymakerAccExtMetrics, PlaymakerAccExtMetrics> {

    private PlaymakerAccExtMetrics requestMetrics;

    public PlaymakerAccExtMeasurement(PlaymakerAccExtMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public PlaymakerAccExtMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public PlaymakerAccExtMetrics getFact() {
        return requestMetrics;
    }

}

