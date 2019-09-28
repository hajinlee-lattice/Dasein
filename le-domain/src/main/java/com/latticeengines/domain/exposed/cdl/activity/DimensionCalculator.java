package com.latticeengines.domain.exposed.cdl.activity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({ @JsonSubTypes.Type(value = DimensionCalculator.class, name = "DimensionCalculator"),
        @JsonSubTypes.Type(value = DimensionCalculatorRegexMode.class, name = "DimensionCalculatorRegexMode") })
public class DimensionCalculator {
    @JsonProperty("name")
    protected String name;

    // target attribute in stream to parse/calculate dimension
    @JsonProperty("attribute")
    protected String attribute;

    public DimensionCalculator() {
        setName(getClass().getSimpleName());
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }
}
