package com.latticeengines.playmaker.measurements;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class PlaymakerPlayMeasurement extends BaseMeasurement<PlaymakerPlayMetrics, PlaymakerPlayMetrics>
        implements Measurement<PlaymakerPlayMetrics, PlaymakerPlayMetrics> {

    private PlaymakerPlayMetrics requestMetrics;

    public PlaymakerPlayMeasurement(PlaymakerPlayMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public PlaymakerPlayMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public PlaymakerPlayMetrics getFact() {
        return requestMetrics;
    }

}
