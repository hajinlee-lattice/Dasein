package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
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

    @JsonProperty("ActivityType")
    private ActivityType type;

    @JsonProperty("Reduced")
    private boolean reduced = false; // true: DepivotedMetrics has complete AID
                                     // + PID for purchase history;
                                     // false: DepivotedMetrics only has AID +
                                     // PID
                                     // which exist in Transaction

    @JsonProperty("AccountHasSegment")
    private boolean accountHasSegment = false;

    @JsonProperty("PeriodTableCount")
    private Integer periodTableCnt = 4;

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

    public ActivityType getType() {
        return type;
    }

    public void setType(ActivityType type) {
        this.type = type;
    }

    public boolean isReduced() {
        return reduced;
    }

    public void setReduced(boolean reduced) {
        this.reduced = reduced;
    }

    public boolean isAccountHasSegment() {
        return accountHasSegment;
    }

    public void setAccountHasSegment(boolean accountHasSegment) {
        this.accountHasSegment = accountHasSegment;
    }

    public Integer getPeriodTableCnt() {
        return periodTableCnt;
    }

    public void setPeriodTableCnt(Integer periodTableCnt) {
        this.periodTableCnt = periodTableCnt;
    }

}
