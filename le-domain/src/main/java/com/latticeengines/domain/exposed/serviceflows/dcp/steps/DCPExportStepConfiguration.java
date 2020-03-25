package com.latticeengines.domain.exposed.serviceflows.dcp.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class DCPExportStepConfiguration extends ImportExportS3StepConfiguration {

    @JsonProperty("upload_pid")
    private Long uploadPid;

    @JsonProperty("source_id")
    private String SourceId;

    @JsonProperty("project_id")
    private String ProjectId;

    public Long getUploadPid() {
        return uploadPid;
    }

    public void setUploadPid(Long uploadPid) {
        this.uploadPid = uploadPid;
    }

    public String getSourceId() {
        return SourceId;
    }

    public void setSourceId(String sourceId) {
        SourceId = sourceId;
    }

    public String getProjectId() {
        return ProjectId;
    }

    public void setProjectId(String projectId) {
        ProjectId = projectId;
    }
}
