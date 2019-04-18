package com.latticeengines.ulysses.monitoring;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

public class UlyssesPurchaseHistMetrics implements Dimension, Fact {
    private Integer getPurchaseHistDurationMS;

    @MetricField(name = "GetPurchaseHistDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getPurchaseHistDurationMS() {
        return getPurchaseHistDurationMS;
    }

    public void setGetPurchaseHistDurationMS(int getPurchaseHistDurationMS) {
        this.getPurchaseHistDurationMS = getPurchaseHistDurationMS;
    }

}
