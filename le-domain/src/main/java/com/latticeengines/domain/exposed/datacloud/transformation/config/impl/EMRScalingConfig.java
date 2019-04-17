package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class EMRScalingConfig extends TransformerConfig {

    @JsonProperty("Operation")
    private Operation operation;

    @JsonProperty("Delta")
    private Integer delta;

    // for jackson
    private EMRScalingConfig() {
    }

    public static EMRScalingConfig scaleOut() {
        EMRScalingConfig config = new EMRScalingConfig();
        config.setOperation(Operation.ScaleOut);
        return config;
    }

    public static EMRScalingConfig scaleIn() {
        EMRScalingConfig config = new EMRScalingConfig();
        config.setOperation(Operation.ScaleIn);
        return config;
    }

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
