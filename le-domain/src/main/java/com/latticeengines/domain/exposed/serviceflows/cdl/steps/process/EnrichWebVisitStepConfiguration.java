package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityImport;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.Catalog;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

public class EnrichWebVisitStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("rematch_mode")
    private boolean isRematchMode;
    @JsonProperty("replace_mode")
    private boolean isReplaceMode;
    @JsonProperty("rebuild_mode")
    private boolean isRebuildMode;
    // streamId -> stream object
    @JsonProperty("activity_stream_map")
    private Map<String, AtlasStream> activityStreamMap;
    @JsonProperty("catalogs")
    private List<Catalog> catalogs;
    @JsonProperty("catalog_imports")
    private Map<String, List<ActivityImport>> catalogImports;
    // streamId -> list({ tableName, original import file name })
    @JsonProperty("stream_imports")
    private Map<String, List<ActivityImport>> streamImports;

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

    public List<Catalog> getCatalogs() {
        return catalogs;
    }

    public void setCatalogs(List<Catalog> catalogs) {
        this.catalogs = catalogs;
    }

    public Map<String, List<ActivityImport>> getStreamImports() {
        return streamImports;
    }

    public void setStreamImports(Map<String, List<ActivityImport>> streamImports) {
        this.streamImports = streamImports;
    }
}
