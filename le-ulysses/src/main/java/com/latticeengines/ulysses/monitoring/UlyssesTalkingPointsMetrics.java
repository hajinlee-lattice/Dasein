package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

public class UlyssesTalkingPointsMetrics implements Dimension, Fact {
    private Integer getTalkingPointsDurationMS;

    @MetricField(name = "GetTalkingPointsDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getTalkingPointsDurationMS() {
        return getTalkingPointsDurationMS;
    }

    public void setGetTalkingPointsDurationMS(int getTalkingPointsDurationMS) {
        this.getTalkingPointsDurationMS = getTalkingPointsDurationMS;
    }

}
