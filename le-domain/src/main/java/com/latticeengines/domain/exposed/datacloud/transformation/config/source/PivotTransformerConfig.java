package com.latticeengines.domain.exposed.datacloud.transformation.config.source;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class PivotTransformerConfig extends TransformerConfig {
    @JsonProperty("JoinFields")
    private String[] joinFields;

    @JsonProperty("BeanName")
    private String beanName;

    public String[] getJoinFields() {
        return joinFields;
    }

    public void setJoinFields(String[] joinFields) {
        this.joinFields = joinFields;
    }

    public String getBeanName() {
        return beanName;
    }

    public void setBeanName(String beanName) {
        this.beanName = beanName;
    }
}
