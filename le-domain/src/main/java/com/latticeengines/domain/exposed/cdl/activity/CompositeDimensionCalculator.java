package com.latticeengines.domain.exposed.cdl.activity;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CompositeDimensionCalculator extends DimensionCalculator {
    private static final long serialVersionUID = 0L;
    public static final String NAME = "CompositeDimensionCalculator";

    @JsonProperty
    public DeriveConfig deriveConfig;
}
