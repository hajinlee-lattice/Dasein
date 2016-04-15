package com.latticeengines.domain.exposed.propdata.publication;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PublicationRequest {

    private String sourceVersion;
    private String submitter;

    @JsonProperty("SourceVersion")
    public String getSourceVersion() {
        return sourceVersion;
    }

    @JsonProperty("SourceVersion")
    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    @JsonProperty("Submitter")
    public String getSubmitter() {
        return submitter;
    }

    @JsonProperty("Submitter")
    public void setSubmitter(String creator) {
        this.submitter = creator;
    }
}
