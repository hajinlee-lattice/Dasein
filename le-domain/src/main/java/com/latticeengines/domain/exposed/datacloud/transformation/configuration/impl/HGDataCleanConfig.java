package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HGDataCleanConfig extends TransformerConfig {

    @JsonProperty("FakedCurrentDate")
    private Date fakedCurrentDate;

    @JsonProperty("DomainField")
    private String domainField;

    public Date getFakedCurrentDate() {
        return fakedCurrentDate;
    }

    public void setFakedCurrentDate(Date fakedCurrentDate) {
        this.fakedCurrentDate = fakedCurrentDate;
    }

    public String getDomainField() {
        return domainField;
    }

    public void setDomainField(String domainField) {
        this.domainField = domainField;
    }

}
