package com.latticeengines.domain.exposed.serviceflows.dcp.steps;

import com.latticeengines.domain.exposed.dcp.DataReportMode;
import com.latticeengines.domain.exposed.dcp.DataReportRecord;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class RollupDataReportStepConfiguration extends MicroserviceStepConfiguration {

    private String root;

    private DataReportRecord.Level level;

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
