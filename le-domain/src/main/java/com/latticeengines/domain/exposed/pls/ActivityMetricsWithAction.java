package com.latticeengines.domain.exposed.pls;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ActivityMetricsWithAction {

    @JsonProperty("metrics")
    private List<ActivityMetrics> metrics;

    @JsonProperty("action")
    private Action action;

    public ActivityMetricsWithAction() {
    }

    public ActivityMetricsWithAction(List<ActivityMetrics> metrics, Action action) {
        this.metrics = metrics;
        this.action = action;
    }

    public List<ActivityMetrics> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<ActivityMetrics> metrics) {
        this.metrics = metrics;
    }

    public Action getAction() {
        return action;
    }

    public void setAction(Action action) {
        this.action = action;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
