package com.latticeengines.domain.exposed.modeling;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;

public class Metadata {

    private String errorMessage;
    private List<AttributeMetadata> attributeMetadata;
    private long status;

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return this.errorMessage;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Metadata")
    public List<AttributeMetadata> getAttributeMetadata() {
        return this.attributeMetadata;
    }

    @JsonProperty("Metadata")
    public void setAttributeMetadata(List<AttributeMetadata> attributeMetadata) {
        this.attributeMetadata = attributeMetadata;
    }

    @JsonProperty("Status")
    public long getStatus() {
        return this.status;
    }

    @JsonProperty("Status")
    public void setStatus(long status) {
        this.status = status;
    }

}
