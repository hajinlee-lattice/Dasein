package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PivotHGDataConfig extends TransformerConfig {
    @JsonProperty("JoinFields")
    private String[] joinFields;

    public String[] getJoinFields() {
        return joinFields;
    }

    public void setJoinFields(String[] joinFields) {
        this.joinFields = joinFields;
    }
}
