package com.latticeengines.perf.domain;

import org.codehaus.jackson.annotate.JsonProperty;

public class SamplingElement implements HasName {

    private int percentage;
    private String name;
    
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
