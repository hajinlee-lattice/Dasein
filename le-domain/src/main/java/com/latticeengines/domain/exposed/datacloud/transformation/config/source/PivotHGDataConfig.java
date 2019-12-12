package com.latticeengines.domain.exposed.datacloud.transformation.config.source;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

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
