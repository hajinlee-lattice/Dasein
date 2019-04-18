package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class UlyssesAccSegmentMetrics implements Dimension, Fact {
    private Integer getAccountSegmentDurationMS;

    private String attributeName;

    @MetricTag(tag = "AttributeName")
    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    @MetricField(name = "GetAccountSegmentDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getAccountSegmentDurationMS() {
        return getAccountSegmentDurationMS;
    }

    public void setGetAccountSegmentDurationMS(int getAccountSegmentDurationMS) {
        this.getAccountSegmentDurationMS = getAccountSegmentDurationMS;
    }

}
