package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class ExportTimelineSparkStepConfiguration extends SparkJobStepConfiguration {

    @JsonProperty("request")
    private TimelineExportRequest request;
    @JsonProperty("timeline_table_names")
    private Map<String, String> timelineTableNames;
    @JsonProperty("lattice_account_table")
    private Table latticeAccountTable;

    public TimelineExportRequest getRequest() {
        return request;
    }

    public void setRequest(TimelineExportRequest request) {
        this.request = request;
    }

    public Map<String, String> getTimelineTableNames() {
        return timelineTableNames;
    }

    public void setTimelineTableNames(Map<String, String> timelineTableNames) {
        this.timelineTableNames = timelineTableNames;
    }

    public Table getLatticeAccountTable() {
        return latticeAccountTable;
    }

    public void setLatticeAccountTable(Table latticeAccountTable) {
        this.latticeAccountTable = latticeAccountTable;
    }
}
