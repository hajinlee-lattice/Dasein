package com.latticeengines.domain.exposed.datacloud.dataflow;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class IntervalBucket extends BucketAlgorithm {

    @Override
    @JsonIgnore
    public String getAlgorithm() {
        return INTEVAL;
    }

    @JsonProperty("boundaries")
    private List<Number> boundaries;

    public List<Number> getBoundaries() {
        return boundaries;
    }

    public void setBoundaries(List<Number> boundaries) {
        this.boundaries = boundaries;
    }
}
