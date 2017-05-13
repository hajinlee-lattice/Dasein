package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PipelineTransformationRequest {

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Version")
    private String version;

    @JsonProperty("KeepTemp")
    private boolean keepTemp;

    @JsonProperty("Steps")
    private List<TransformationStepConfig> steps;

    @JsonProperty("EnableSlack")
    private boolean enableSlack;

    @JsonProperty("ContainerMemMB")
    private Integer containerMemMB;

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean getKeepTemp() {
        return keepTemp;
    }

    public void setKeepTemp(boolean keepTemp) {
        this.keepTemp = keepTemp;
    }

    public List<TransformationStepConfig> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepConfig> steps) {
        this.steps = steps;
    }

    public boolean isEnableSlack() {
        return enableSlack;
    }

    public void setEnableSlack(boolean enableSlack) {
        this.enableSlack = enableSlack;
    }

    public Integer getContainerMemMB() {
        return containerMemMB;
    }

    public void setContainerMemMB(Integer containerMemMB) {
        this.containerMemMB = containerMemMB;
    }
}
