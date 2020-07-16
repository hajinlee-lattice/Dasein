package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DCPReportRequest {

    @JsonProperty("root")
    private String root;

    @JsonProperty("level")
    private DataReportRecord.Level level;

    @JsonProperty("mode")
    private DataReportMode mode;

    public String getRoot() {
        return root;
    }

    public void setRoot(String root) {
        this.root = root;
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
