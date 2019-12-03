package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.metadata.transaction.Product;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsPivotConfig extends TransformerConfig {
    @JsonProperty("ActivityType")
    private ActivityType activityType;

    @JsonProperty("ProductMap")
    private Map<String, List<Product>> productMap;

    @JsonProperty("GroupByField")
    private String groupByField;

    @JsonProperty("PivotField")
    private String pivotField;

    @JsonProperty("Metrics")
    private List<ActivityMetrics> metrics;

    @JsonProperty("Expanded")
    private boolean expanded = false; // true: need to join Account table to
                                      // include all AID
                                      // false: DepivotedMetrics already has all
                                      // AID

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

    public boolean isExpanded() {
        return expanded;
    }

    public void setExpanded(boolean expanded) {
        this.expanded = expanded;
    }

    public List<ActivityMetrics> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<ActivityMetrics> metrics) {
        this.metrics = metrics;
    }

}
