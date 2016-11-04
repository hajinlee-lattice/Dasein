package com.latticeengines.domain.exposed.actors;

import java.util.List;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

public class MeasurementMessage<M extends Measurement<?, ?>> {

    private List<M> measurements;
    private MetricDB metricDB;

    public List<M> getMeasurements() {
        return measurements;
    }

    public void setMeasurements(List<M> measurements) {
        this.measurements = measurements;
    }

    public MetricDB getMetricDB() {
        return metricDB;
    }

    public void setMetricDB(MetricDB metricDB) {
        this.metricDB = metricDB;
    }
}
