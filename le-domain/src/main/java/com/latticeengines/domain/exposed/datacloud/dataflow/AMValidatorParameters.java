package com.latticeengines.domain.exposed.datacloud.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMValidatorParameters extends TransformationFlowParameters {

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("Duns")
    private String duns;

    @JsonProperty("LatticeId")
    private String latticeId;

    @JsonProperty("AmCount")
    private Long amCount;

    public Long getAmCount() {
        return amCount;
    }

    public void setAmCount(Long amCount) {
        this.amCount = amCount;
    }

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

}
