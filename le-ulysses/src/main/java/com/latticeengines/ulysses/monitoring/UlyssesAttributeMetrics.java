package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class UlyssesAttributeMetrics implements Dimension, Fact {
    private Integer getAttributeDurationMS;

    private String attributeName;

    @MetricTag(tag = "AttributeName")
    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    @MetricField(name = "GetAttributeDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getAttributeDurationMS() {
        return getAttributeDurationMS;
    }

    public void setGetAttributeDurationMS(int getAttributeDurationMS) {
        this.getAttributeDurationMS = getAttributeDurationMS;
    }

}
