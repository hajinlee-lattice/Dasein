package com.latticeengines.domain.exposed.propdata.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class IngestionRequest {
    private String submitter;
    private String fileName;

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

}
