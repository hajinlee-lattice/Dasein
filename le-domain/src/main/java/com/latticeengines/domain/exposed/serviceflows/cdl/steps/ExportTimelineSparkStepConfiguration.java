package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.TimelineExportRequest;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class ExportTimelineSparkStepConfiguration extends SparkJobStepConfiguration {

    @JsonProperty("request")
    private TimelineExportRequest request;
    @JsonProperty("timeline_table_names")
    private Map<String, String> timelineTableNames;
    @JsonProperty("version")
    private DataCollection.Version version;

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

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }
}
