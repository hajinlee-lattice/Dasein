package com.latticeengines.domain.exposed.pls;

import com.fasterxml.jackson.annotation.JsonProperty;


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
