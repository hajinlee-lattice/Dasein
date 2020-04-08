package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class ActivityStreamSparkStepConfiguration extends SparkJobStepConfiguration {

    // streamId -> stream object
    @JsonProperty("activity_stream_map")
    private Map<String, AtlasStream> activityStreamMap;

    // groupId -> metricsGroup obj
    @JsonProperty("activity_metrics_group_map")
    private Map<String, ActivityMetricsGroup> activityMetricsGroupMap;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @JsonProperty("entity_match_ga_only")
    private boolean entityMatchGAOnly;

    @JsonProperty("rebuild_activity_store")
    private boolean shouldRebuild;

    public Map<String, AtlasStream> getActivityStreamMap() {
        return activityStreamMap;
    }

    public void setActivityStreamMap(Map<String, AtlasStream> activityStreamMap) {
        this.activityStreamMap = activityStreamMap;
    }

    public Map<String, ActivityMetricsGroup> getActivityMetricsGroupMap() {
        return activityMetricsGroupMap;
    }

    public void setActivityMetricsGroupMap(Map<String, ActivityMetricsGroup> activityMetricsGroupMap) {
        this.activityMetricsGroupMap = activityMetricsGroupMap;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }

    public boolean isEntityMatchGAOnly() {
        return entityMatchGAOnly;
    }

    public void setEntityMatchGAOnly(boolean entityMatchGAOnly) {
        this.entityMatchGAOnly = entityMatchGAOnly;
    }

    public boolean isShouldRebuild() {
        return shouldRebuild;
    }

    public void setShouldRebuild(boolean shouldRebuild) {
        this.shouldRebuild = shouldRebuild;
    }
}
