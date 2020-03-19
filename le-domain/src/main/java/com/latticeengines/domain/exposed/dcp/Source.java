package com.latticeengines.domain.exposed.dcp;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeedTask;

public class Source {

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("source_display_name")
    private String sourceDisplayName;

    @JsonProperty("relative_path")
    private String relativePath;

    @JsonProperty("full_path")
    private String fullPath;

    @JsonProperty("import_status")
    private DataFeedTask.S3ImportStatus importStatus;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceDisplayName() {
        return sourceDisplayName;
    }

    public void setSourceDisplayName(String sourceDisplayName) {
        this.sourceDisplayName = sourceDisplayName;
    }

    public String getRelativePath() {
        return relativePath;
    }

    public void setRelativePath(String relativePath) {
        this.relativePath = relativePath;
    }

    public String getFullPath() {
        return fullPath;
    }

    public void setFullPath(String fullPath) {
        this.fullPath = fullPath;
    }

    public DataFeedTask.S3ImportStatus getImportStatus() {
        return importStatus;
    }

    public void setImportStatus(DataFeedTask.S3ImportStatus importStatus) {
        this.importStatus = importStatus;
    }
}
