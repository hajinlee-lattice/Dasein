package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DCPReportRequest {

    /**
     * the root id value varied in different level
     * tenant -> tenant Id
     * project -> project Id
     * source -> source Id
     * upload -> upload Id
     */
    @JsonProperty("rootId")
    private String rootId;

    @JsonProperty("level")
    private DataReportRecord.Level level;

    @JsonProperty("mode")
    private DataReportMode mode;

    public String getRootId() {
        return rootId;
    }

    public void setRootId(String rootId) {
        this.rootId = rootId;
    }

    public DataReportRecord.Level getLevel() {
        return level;
    }

    public void setLevel(DataReportRecord.Level level) {
        this.level = level;
    }

    public DataReportMode getMode() {
        return mode;
    }

    public void setMode(DataReportMode mode) {
        this.mode = mode;
    }
}
