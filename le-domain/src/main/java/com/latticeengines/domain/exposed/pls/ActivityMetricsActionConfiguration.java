package com.latticeengines.domain.exposed.pls;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ActivityMetricsActionConfiguration extends ActionConfiguration {

    public ActivityMetricsActionConfiguration() {

    }

    public ActivityMetricsActionConfiguration(List<String> activated, List<String> updated, List<String> deactivated) {
        this.activated = activated;
        this.updated = updated;
        this.deactivated = deactivated;
    }

    @JsonProperty("Activated")
    private List<String> activated;

    @JsonProperty("Updated")
    private List<String> updated;

    @JsonProperty("Deactivated")
    private List<String> deactivated;

    public List<String> getActivated() {
        return activated;
    }

    public void setActivated(List<String> activated) {
        this.activated = activated;
    }

    public List<String> getUpdated() {
        return updated;
    }

    public void setUpdated(List<String> updated) {
        this.updated = updated;
    }

    public List<String> getDeactivated() {
        return deactivated;
    }

    public void setDeactivated(List<String> deactivated) {
        this.deactivated = deactivated;
    }

    @Override
    public String serialize() {
        List<String> confStr = new ArrayList<>();
        if (CollectionUtils.isNotEmpty(activated)) {
            confStr.add("Activated: " + String.join(",", activated));
        }
        if (CollectionUtils.isNotEmpty(updated)) {
            confStr.add("Updated: " + String.join(",", updated));
        }
        if (CollectionUtils.isNotEmpty(deactivated)) {
            confStr.add("Deactivated: " + String.join(",", deactivated));
        }
        return String.join("; ", confStr);
    }
}
