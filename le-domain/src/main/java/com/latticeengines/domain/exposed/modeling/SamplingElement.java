package com.latticeengines.domain.exposed.modeling;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class SamplingElement implements HasName {

    private int percentage;
    private String name;

    @JsonCreator
    public SamplingElement() {
    }

    @JsonCreator
    public SamplingElement(String name) {
        this.name = name;
    }

    @JsonProperty("percentage")
    public int getPercentage() {
        return percentage;
    }

    @JsonProperty("percentage")
    public void setPercentage(int percentage) {
        this.percentage = percentage;
    }

    @JsonProperty("name")
    @Override
    public String getName() {
        return name;
    }

    @JsonProperty("name")
    @Override
    public void setName(String name) {
        this.name = name;
    }

}
