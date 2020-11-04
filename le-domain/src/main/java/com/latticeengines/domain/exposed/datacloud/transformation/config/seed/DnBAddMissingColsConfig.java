package com.latticeengines.domain.exposed.datacloud.transformation.config.seed;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class DnBAddMissingColsConfig extends TransformerConfig {

    @JsonProperty("DomainField")
    private String domain;

    @JsonProperty("DunsField")
    private String duns;

    @JsonProperty("FieldsToRemove")
    private List<String> fieldsToRemove;

    @JsonProperty("FieldsToPopulateNull")
    private List<String> fieldsToPopulateNull;

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

    public List<String> getFieldsToRemove() {
        return fieldsToRemove;
    }

    public void setFieldsToRemove(List<String> fieldsToRemove) {
        this.fieldsToRemove = fieldsToRemove;
    }

    public List<String> getFieldsToPopulateNull() {
        return fieldsToPopulateNull;
    }

    public void setFieldsToPopulateNull(List<String> fieldsToPopulateNull) {
        this.fieldsToPopulateNull = fieldsToPopulateNull;
    }
}
