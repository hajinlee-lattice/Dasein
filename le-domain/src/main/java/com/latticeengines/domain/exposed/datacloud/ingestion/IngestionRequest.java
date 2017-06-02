package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestionRequest {
    private String submitter;
    private String fileName; // Only for Ingestion type: SFTP
    private String sourceVersion; // Only for Ingestion type: SQL_TO_SOURCE

    @JsonProperty("Submitter")
    public String getSubmitter() {
        return submitter;
    }

    @JsonProperty("Submitter")
    public void setSubmitter(String submitter) {
        this.submitter = submitter;
    }

    @JsonProperty("FileName")
    public String getFileName() {
        return fileName;
    }

    @JsonProperty("FileName")
    public void setFileName(String fileName) {
        this.fileName = fileName;
    }

    @JsonProperty("SourceVersion")
    public String getSourceVersion() {
        return sourceVersion;
    }

    @JsonProperty("SourceVersion")
    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

}
