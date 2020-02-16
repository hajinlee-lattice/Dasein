package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class ExportToDynamoStepConfiguration extends MicroserviceStepConfiguration {

    @NotNull
    @JsonProperty("dynamoSignature")
    private String dynamoSignature;

    @JsonProperty("dynamoSignature")
    private Boolean onlyUpdateSignature;

    public String getDynamoSignature() {
        return dynamoSignature;
    }

    public void setDynamoSignature(String dynamoSignature) {
        this.dynamoSignature = dynamoSignature;
    }

    public Boolean getOnlyUpdateSignature() {
        return onlyUpdateSignature;
    }

    public void setOnlyUpdateSignature(Boolean onlyUpdateSignature) {
        this.onlyUpdateSignature = onlyUpdateSignature;
    }
}
