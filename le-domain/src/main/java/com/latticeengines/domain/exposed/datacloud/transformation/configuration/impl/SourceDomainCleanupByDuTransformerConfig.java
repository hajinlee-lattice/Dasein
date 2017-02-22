package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SourceDomainCleanupByDuTransformerConfig extends TransformerConfig {

    @JsonProperty("DomainField")
    private String domainField;
    @JsonProperty("DunsField")
    private String dunsField;
    @JsonProperty("duField")
    private String duField;
    @JsonProperty("alexaRankField")
    private String alexaRankField;
    
    public String getDomainField() {
        return domainField;
    }
    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }
    public String getDunsField() {
        return this.dunsField;
    }
    public void setDunsField(String dunsField) {
        this.dunsField = dunsField;
    }
    public String getDuField() {
        return this.duField;
    }
    public void setDuField(String duField) {
        this.duField = duField;
    }
    public String getAlexaRankField() {
        return alexaRankField;
    }
    public void setAlexaRankField(String alexaRankField) {
        this.alexaRankField = alexaRankField;
    }
    
}
