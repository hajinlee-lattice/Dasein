package com.latticeengines.domain.exposed.dataplatform.visidb;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.modeling.ModelingMetadata.AttributeMetadata;

public class GetQueryMetaDataColumnsResponse {

    private String errorMessage;
    private List<AttributeMetadata> metadata = new ArrayList<>();
    private int status;
    
    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    @JsonProperty("ErrorMessage")
    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Metadata")
    public List<AttributeMetadata> getMetadata() {
        return metadata;
    }

    @JsonProperty("Metadata")
    public void setMetadata(List<AttributeMetadata> metadata) {
        this.metadata = metadata;
    }

    @JsonProperty("Status")
    public int getStatus() {
        return status;
    }

    @JsonProperty("Status")
    public void setStatus(int status) {
        this.status = status;
    }
    
    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
