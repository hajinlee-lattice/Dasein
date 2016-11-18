package com.latticeengines.domain.exposed.datacloud.transformation;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformationStepConfig {

    @JsonProperty("Transformer")
    private String transformer;

    @JsonProperty("InputStep")
    private Integer inputStep;

    @JsonProperty("TargetSource")
    private String targetSource;

    @JsonProperty("TargetTemplate")
    private String targetTemplate;

    @JsonProperty("TargetVersion")
    private String targetVersion;

    @JsonProperty("Configuration")
    private String configuration;

    public String getTransformer() {
        return transformer;
    }

    public void setTransformer(String transformer) {
        this.transformer = transformer;
    }

    public String getConfiguration() {
        return configuration;
    }

    public void setConfiguration(String configuration) {
        this.configuration = configuration;
    }

    public String getTargetSource() {
        return targetSource;
    }

    public void setTargetSource(String targetSource) {
        this.targetSource = targetSource;
    }

    public String getTargetTemplate() {
        return targetTemplate;
    }

    public void setTargetTemplate(String targetTemplate) {
        this.targetTemplate = targetTemplate;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public Integer getInputStep() {
       return inputStep;
    }

    public void setInputStep(Integer inputStep) {
       this.inputStep = inputStep;
    }

}
