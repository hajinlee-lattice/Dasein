package com.latticeengines.scoringapi.exposed;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wordnik.swagger.annotations.ApiModelProperty;

public class AccountScoreRequest {

    @JsonProperty("modelId")
    private String modelId;

    @JsonProperty("website")
    @ApiModelProperty(required = true)
    private String website;

    @JsonProperty("company")
    private Company company;

    @JsonProperty("fields")
    private List<Field> fields;

    public String getModelId() {
        return modelId;
    }

    public void setModelId(String modelId) {
        this.modelId = modelId;
    }

    public String getWebsite() {
        return website;
    }

    public void setWebsite(String website) {
        this.website = website;
    }

    public Company getCompany() {
        return company;
    }

    public void setCompany(Company company) {
        this.company = company;
    }

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

}
