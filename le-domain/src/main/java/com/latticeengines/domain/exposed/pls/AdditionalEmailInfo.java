package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AdditionalEmailInfo {

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("model_id")
    private String modelId;

    public AdditionalEmailInfo(){

    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }
}
