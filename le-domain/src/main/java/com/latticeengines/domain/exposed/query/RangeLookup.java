package com.latticeengines.domain.exposed.query;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RangeLookup extends Lookup {
    @JsonProperty("min")
    private Object min;

    @JsonProperty("max")
    private Object max;

    public RangeLookup(Object min, Object max) {
        this.min = min;
        this.max = max;
    }

    public RangeLookup() {
    }

    public Object getMin() {
        return min;
    }

    public void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    public void setMax(Object max) {
        this.max = max;
    }
}
