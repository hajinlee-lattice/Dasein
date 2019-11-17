package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.WRAPPER_OBJECT)
@JsonSubTypes({
        @JsonSubTypes.Type(value = ProductValidationSummary.class, name = "ProductValidationSummary")})
public class EntityValidationSummary {

    @JsonProperty("error_line_number")
    private long errorLineNumber;

    public long getErrorLineNumber() {
        return errorLineNumber;
    }

    public void setErrorLineNumber(long errorLineNumber) {
        this.errorLineNumber = errorLineNumber;
    }
}
