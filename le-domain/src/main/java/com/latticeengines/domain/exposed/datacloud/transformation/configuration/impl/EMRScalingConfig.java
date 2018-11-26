package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EMRScalingConfig extends TransformerConfig {

    @JsonProperty("Operation")
    private Operation operation;

    @JsonProperty("Delta")
    private Integer delta;

    public Operation getOperation() {
        return operation;
    }

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Integer getDelta() {
        return delta;
    }

    public void setDelta(Integer delta) {
        this.delta = delta;
    }

    public enum Operation {
        ScaleOut, ScaleIn
    }

}
