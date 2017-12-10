package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMValidatorParams extends TransformationFlowParameters {
    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("Duns")
    private String duns;

    @JsonProperty("LatticeId")
    private String latticeId;

    @JsonProperty("Key")
    private String key;

    @JsonProperty("DiffVersion")
    private String diffVersion;

    @JsonProperty("DiffVersionCompared")
    private String diffVersionCompared;

    @JsonProperty("checkNotNullField")
    private String checkNotNullField;

    @JsonProperty("checkNullField")
    private String checkNullField;

    @JsonProperty("threshold")
    private double threshold;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public String getLatticeId() {
        return latticeId;
    }

    public void setLatticeId(String latticeId) {
        this.latticeId = latticeId;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getDiffVersion() {
        return diffVersion;
    }

    public void setDiffVersion(String diffVersion) {
        this.diffVersion = diffVersion;
    }

    public String getDiffVersionCompared() {
        return diffVersionCompared;
    }

    public void setDiffVersionCompared(String diffVersionCompared) {
        this.diffVersionCompared = diffVersionCompared;
    }

    public String getCheckNotNullField() {
        return checkNotNullField;
    }

    public void setNotNullField(String checkNotNullField) {
        this.checkNotNullField = checkNotNullField;
    }

    public String getCheckNullField() {
        return checkNullField;
    }

    public void setNullField(String checkNullField) {
        this.checkNullField = checkNullField;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

}
