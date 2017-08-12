package com.latticeengines.domain.exposed.datacloud.publication;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class PublicationRequest {

    @JsonProperty("SourceVersion")
    private String sourceVersion;

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("Destination")
    private PublicationDestination destination;

    public String getSourceVersion() {
        return sourceVersion;
    }

    public void setSourceVersion(String sourceVersion) {
        this.sourceVersion = sourceVersion;
    }

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public PublicationDestination getDestination() {
        return destination;
    }

    public void setDestination(PublicationDestination destination) {
        this.destination = destination;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
