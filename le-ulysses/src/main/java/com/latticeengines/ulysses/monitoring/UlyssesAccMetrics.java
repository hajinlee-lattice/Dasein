package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class UlyssesAccMetrics implements Dimension, Fact {
    private Integer getAccountDurationMS;

    private String attributeName;

    @MetricTag(tag = "AttributeName")
    public String getAttributeName() {
        return attributeName;
    }

    public void setAttributeName(String attributeName) {
        this.attributeName = attributeName;
    }

    @MetricField(name = "GetAccountDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getAccountDurationMS() {
        return getAccountDurationMS;
    }

    public void setGetAccountDurationMS(int getAccountDurationMS) {
        this.getAccountDurationMS = getAccountDurationMS;
    }

}
