package com.latticeengines.playmaker.measurements;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.BaseMeasurement;

public class PlaymakerRecMeasurement extends BaseMeasurement<PlaymakerRecMetrics, PlaymakerRecMetrics>
        implements Measurement<PlaymakerRecMetrics, PlaymakerRecMetrics> {

    private PlaymakerRecMetrics requestMetrics;

    public PlaymakerRecMeasurement(PlaymakerRecMetrics requestMetrics) {
        this.requestMetrics = requestMetrics;
    }

    @Override
    public PlaymakerRecMetrics getDimension() {
        return requestMetrics;
    }

    @Override
    public PlaymakerRecMetrics getFact() {
        return requestMetrics;
    }

}
