package com.latticeengines.monitor.metric.measurement;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class InspectionDimension implements Dimension {

    private final String name;

    InspectionDimension(String name) {
        this.name = name;
    }

    @MetricTag(tag = "Inspection")
    public String name() {
        return name;
    }

}
