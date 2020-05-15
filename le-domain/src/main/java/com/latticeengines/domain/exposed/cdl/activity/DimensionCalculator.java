package com.latticeengines.domain.exposed.cdl.activity;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
@JsonSubTypes({
        @JsonSubTypes.Type(value = DimensionCalculator.class, name = "DimensionCalculator"),
        @JsonSubTypes.Type(value = DimensionCalculatorRegexMode.class, name = "DimensionCalculatorRegexMode"),
        @JsonSubTypes.Type(value = DimensionCalculatorExistence.class, name = "DimensionCalculatorExistence")
})
public class DimensionCalculator implements Serializable {

    private static final long serialVersionUID = -4708499649443194248L;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DimensionCalculator that = (DimensionCalculator) o;
        return Objects.equal(name, that.name) && Objects.equal(attribute, that.attribute);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name, attribute);
    }
}
