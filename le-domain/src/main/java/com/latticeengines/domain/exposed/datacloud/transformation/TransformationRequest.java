package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TransformationRequest {

    @JsonProperty("SourceBeanName")
    private String sourceBeanName;

    @JsonProperty("Submitter")
    private String submitter;

    @JsonProperty("BaseVersions")
    private List<String> baseVersions;

    @JsonProperty("TargetVersion")
    private String targetVersion;

    public String getSourceBeanName() {
        return sourceBeanName;
    }

    public void setSourceBeanName(String sourceBeanName) {
        this.sourceBeanName = sourceBeanName;
    }

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public List<String> getBaseVersions() {
        return baseVersions;
    }

    public void setBaseVersions(List<String> baseVersions) {
        this.baseVersions = baseVersions;
    }

    public String getTargetVersion() {
        return targetVersion;
    }

    public void setTargetVersion(String targetVersion) {
        this.targetVersion = targetVersion;
    }
}
