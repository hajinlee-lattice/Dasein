package com.latticeengines.domain.exposed.serviceflows.dcp.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ImportSourceStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("upload_pid")
    private Long uploadPid;

    @JsonProperty("stats_pid")
    private Long statsPid;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public Long getUploadPid() {
        return uploadPid;
    }

    public void setUploadPid(Long uploadPid) {
        this.uploadPid = uploadPid;
    }

    public Long getStatsPid() {
        return statsPid;
    }

    public void setStatsPid(Long statsPid) {
        this.statsPid = statsPid;
    }
}
