package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class EnrichWebVisitSparkStepConfiguration extends SparkJobStepConfiguration {

    @JsonProperty("rematch_mode")
    private boolean isRematchMode;
    @JsonProperty("replace_mode")
    private boolean isReplaceMode;
    @JsonProperty("rebuild_mode")
    private boolean isRebuildMode;
    // streamId -> stream object
    @JsonProperty("activity_stream_map")
    private Map<String, AtlasStream> activityStreamMap;
    @JsonProperty("catalog_imports")
    private Map<String, List<ActivityImport>> catalogImports;

    public boolean isRematchMode() {
        return isRematchMode;
    }

    public void setRematchMode(boolean rematchMode) {
        isRematchMode = rematchMode;
    }

    public boolean isReplaceMode() {
        return isReplaceMode;
    }

    public void setReplaceMode(boolean replaceMode) {
        isReplaceMode = replaceMode;
    }

    public boolean isRebuildMode() {
        return isRebuildMode;
    }

    public void setRebuildMode(boolean rebuildMode) {
        isRebuildMode = rebuildMode;
    }

    public Map<String, AtlasStream> getActivityStreamMap() {
        return activityStreamMap;
    }

    public void setActivityStreamMap(Map<String, AtlasStream> activityStreamMap) {
        this.activityStreamMap = activityStreamMap;
    }

    public Map<String, List<ActivityImport>> getCatalogImports() {
        return catalogImports;
    }

    public void setCatalogImports(Map<String, List<ActivityImport>> catalogImports) {
        this.catalogImports = catalogImports;
    }
}
