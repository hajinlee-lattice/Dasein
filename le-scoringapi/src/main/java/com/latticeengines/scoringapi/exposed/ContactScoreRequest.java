package com.latticeengines.scoringapi.exposed;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.wordnik.swagger.annotations.ApiModelProperty;

public class ContactScoreRequest {

    @JsonProperty("modelId")
    private String modelId;

    @JsonProperty("emailAddress")
    @ApiModelProperty(required = true)
    private String emailAddress;

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

    public String getEmailAddress() {
        return emailAddress;
    }

    public void setEmailAddress(String emailAddress) {
        this.emailAddress = emailAddress;
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
