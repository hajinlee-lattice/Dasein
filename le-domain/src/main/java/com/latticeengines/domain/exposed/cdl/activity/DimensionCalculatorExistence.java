package com.latticeengines.domain.exposed.cdl.activity;


import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Generate boolean dimension if record exists
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DimensionCalculatorExistence extends DimensionCalculator {

    private static final long serialVersionUID = 0L;

    @JsonProperty("targetBoolean")
    // target boolean value if record exists
    private Boolean targetBoolean;

    public Boolean getTargetBoolean() {
        return targetBoolean;
    }

    public void setTargetBoolean(Boolean targetBoolean) {
        this.targetBoolean = targetBoolean;
    }
}
