package com.latticeengines.domain.exposed.serviceflows.dcp.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class RollupDataReportStepConfiguration extends MicroserviceStepConfiguration {

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
