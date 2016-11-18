package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class PipelineTransformationRequest {

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("BaseSources")
    private List<String> baseSources;

    @JsonProperty("BaseVersions")
    private List<String> baseVersions;

    @JsonProperty("BaseTemplates")
    private List<String> baseTemplates;

    @JsonProperty("TargetSource")
    private String targetSource;

    @JsonProperty("TargetVersion")
    private String targetVersion;

    @JsonProperty("TargetTemplate")
    private String targetTemplate;

    @JsonProperty("Steps")
    private List<TransformationStepConfig> steps;

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public List<String> getBaseSources() {
        return baseSources;
    }

    public void setBaseSources(List<String> baseSources) {
        this.baseSources = baseSources;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public void setBaseTemplates(List<String> baseTemplates) {
        this.baseTemplates = baseTemplates;
    }

    public List<String> getBaseTemplates() {
        return baseTemplates;
    }

    public void setTargetSource(String targetSource) {
        this.targetSource = targetSource;
    }

    public String getTargetSource() {
        return targetSource;
    }

    public void setTargetTemplate(String targetTemplate) {
        this.targetTemplate = targetTemplate;
    }

    public String getTargetTemplate() {
        return targetTemplate;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public List<TransformationStepConfig> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepConfig> steps) {
        this.steps = steps;
    }
}
