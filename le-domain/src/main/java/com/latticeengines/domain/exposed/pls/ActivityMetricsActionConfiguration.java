package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ActivityMetricsActionConfiguration extends ActionConfiguration {

    public ActivityMetricsActionConfiguration() {

    }

    public ActivityMetricsActionConfiguration(int newCnt, int activateCnt, int deprecateCnt) {
        this.newCnt = newCnt;
        this.activateCnt = activateCnt;
        this.deprecateCnt = deprecateCnt;
    }

    @JsonProperty("NewCnt")
    private int newCnt;

    @JsonProperty("ActivateCnt")
    private int activateCnt;

    @JsonProperty("DeprecateCnt")
    private int deprecateCnt;

    public int getNewCnt() {
        return newCnt;
    }

    public void setNewCnt(int newCnt) {
        this.newCnt = newCnt;
    }

    public int getActivateCnt() {
        return activateCnt;
    }

    public void setActivateCnt(int activateCnt) {
        this.activateCnt = activateCnt;
    }

    public int getDeprecateCnt() {
        return deprecateCnt;
    }

    public void setDeprecateCnt(int deprecateCnt) {
        this.deprecateCnt = deprecateCnt;
    }

    @Override
    public String serialize() {
        return String.format("%d metrics are created, %d metrics are re-activated and %d metrics are deprecated",
                newCnt, activateCnt, deprecateCnt);
    }
}
