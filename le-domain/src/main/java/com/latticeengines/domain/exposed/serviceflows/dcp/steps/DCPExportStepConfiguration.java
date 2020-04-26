package com.latticeengines.domain.exposed.serviceflows.dcp.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.ImportExportS3StepConfiguration;

public class DCPExportStepConfiguration extends ImportExportS3StepConfiguration {

    @JsonProperty("upload_id")
    private String uploadId;

    @JsonProperty("source_id")
    private String SourceId;

    @JsonProperty("project_id")
    private String ProjectId;

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
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
