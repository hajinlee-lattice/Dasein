package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class AMSeedSecondDomainCleanupConfig extends TransformerConfig {
    @JsonProperty("DomainField")
    private String domainField;

    @JsonProperty("DunsField")
    private String dunsField;

    @JsonProperty("SecondDomainField")
    private String secondDomainField;

    public String getDomainField() {
        return domainField;
    }

    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }

    public String getDunsField() {
        return dunsField;
    }

    public void setDunsField(String dunsField) {
        this.dunsField = dunsField;
    }

    public String getSecondDomainField() {
        return secondDomainField;
    }

    public void setSecondDomainField(String secondDomainField) {
        this.secondDomainField = secondDomainField;
    }

}
