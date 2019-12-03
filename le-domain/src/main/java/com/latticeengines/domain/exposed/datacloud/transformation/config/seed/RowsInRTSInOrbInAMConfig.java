package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class RowsInRTSInOrbInAMConfig extends TransformerConfig {
    @JsonProperty("DomainSource")
    private String domainSource;

    @JsonProperty("Domain")
    private String domain;

    @JsonProperty("SecondaryDomain")
    private String secDomain;

    public String getDomainSource() {
        return domainSource;
    }

    public void setDomainSource(String domainSource) {
        this.domainSource = domainSource;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getSecDomain() {
        return secDomain;
    }

    public void setSecDomain(String secDomain) {
        this.secDomain = secDomain;
    }
}
