package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.Product;

public class ActivityMetricsPivotConfig extends TransformerConfig {
    @JsonProperty("ActivityType")
    private ActivityType activityType;

    @JsonProperty("ProductMap")
    private Map<String, List<Product>> productMap;

    @JsonProperty("GroupByField")
    private String groupByField;

    @JsonProperty("PivotField")
    private String pivotField;

    public ActivityType getActivityType() {
        return activityType;
    }

    public void setActivityType(ActivityType activityType) {
        this.activityType = activityType;
    }

    public Map<String, List<Product>> getProductMap() {
        return productMap;
    }

    public void setProductMap(Map<String, List<Product>> productMap) {
        this.productMap = productMap;
    }

    public String getGroupByField() {
        return groupByField;
    }

    public void setGroupByField(String groupByField) {
        this.groupByField = groupByField;
    }

    public String getPivotField() {
        return pivotField;
    }

    public void setPivotField(String pivotField) {
        this.pivotField = pivotField;
    }

}
