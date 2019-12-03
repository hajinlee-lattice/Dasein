package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;

public class DomainPatchConfig extends TransformerConfig {
    @NotNull
    @NotEmptyString
    @JsonProperty("DomainSource")
    private String domainSource;

    public String getDomainSource() {
        return domainSource;
    }

    public void setDomainSource(String domainSource) {
        this.domainSource = domainSource;
    }

}
