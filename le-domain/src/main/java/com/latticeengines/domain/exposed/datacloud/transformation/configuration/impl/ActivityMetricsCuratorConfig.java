package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

public class ActivityMetricsCuratorConfig extends TransformerConfig {
    @JsonProperty("Metrics")
    private List<ActivityMetrics> metrics;

    @JsonProperty("PeriodStrategy")
    private List<PeriodStrategy> periodStrategies;

    @JsonProperty("GroupByFields")
    private List<String> groupByFields;

    @JsonProperty("CurrentDate")
    private String currentDate;

    public List<ActivityMetrics> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<ActivityMetrics> metrics) {
        this.metrics = metrics;
    }

    public List<PeriodStrategy> getPeriodStrategies() {
        return periodStrategies;
    }

    public void setPeriodStrategies(List<PeriodStrategy> periodStrategies) {
        this.periodStrategies = periodStrategies;
    }

    public List<String> getGroupByFields() {
        return groupByFields;
    }

    public void setGroupByFields(List<String> groupByFields) {
        this.groupByFields = groupByFields;
    }

    public String getCurrentDate() {
        return currentDate;
    }

    public void setCurrentDate(String currentDate) {
        this.currentDate = currentDate;
    }
}
