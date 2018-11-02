package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestionRequest {

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("StartNow")
    private Boolean startNow;

    @JsonProperty("FileName")
    private String fileName; // Only for Ingestion type: SFTP

    @JsonProperty("SourceVersion")
    private String sourceVersion; // Only for Ingestion type: SQL_TO_SOURCE, S3,
                                  // PATCH_BOOK (datacloud versions)

    @JsonProperty("UpdateCurrentVersion")
    private Boolean updateCurrentVersion; // Only for Ingestion type: S3

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String submitter) {
        this.submitter = submitter;
    }

    public Boolean getStartNow() {
        return startNow;
    }

    public void setStartNow(Boolean startNow) {
        this.startNow = startNow;
    }

    public String getFileName() {
        return fileName;
    }

    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public Boolean getUpdateCurrentVersion() {
        return updateCurrentVersion;
    }

    public void setUpdateCurrentVersion(Boolean updateCurrentVersion) {
        this.updateCurrentVersion = updateCurrentVersion;
    }

}
