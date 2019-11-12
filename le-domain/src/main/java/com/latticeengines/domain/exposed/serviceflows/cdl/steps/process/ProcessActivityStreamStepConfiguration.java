package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessActivityStreamStepConfiguration extends BaseProcessEntityStepConfiguration {

    // streamId -> stream object
    @JsonProperty("activity_stream_map")
    private Map<String, AtlasStream> activityStreamMap;

    // groupId -> metricsGroup obj
    @JsonProperty("activity_metrics_group_map")
    private Map<String, ActivityMetricsGroup> activityMetricsGroupMap;

    // streamId -> list({ tableName, original import file name })
    @JsonProperty("stream_imports")
    private Map<String, List<ActivityImport>> streamImports;

    // streamId -> raw stream table in current active version
    @JsonProperty("active_raw_stream_tables")
    private Map<String, String> activeRawStreamTables;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @JsonProperty("entity_match_ga_only")
    private boolean entityMatchGAOnly;

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

    public Map<String, List<ActivityImport>> getStreamImports() {
        return streamImports;
    }

    public void setStreamImports(Map<String, List<ActivityImport>> streamImports) {
        this.streamImports = streamImports;
    }

    public Map<String, String> getActiveRawStreamTables() {
        return activeRawStreamTables;
    }

    public void setActiveRawStreamTables(Map<String, String> activeRawStreamTables) {
        this.activeRawStreamTables = activeRawStreamTables;
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

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.ActivityStream;
    }
}
