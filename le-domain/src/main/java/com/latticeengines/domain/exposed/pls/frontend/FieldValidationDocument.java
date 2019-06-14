package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldValidationDocument {

    @JsonProperty
    private List<FieldValidation> validations;

    public List<FieldValidation> getValidations() {
        return validations;
    }

    public void setValidations(List<FieldValidation> validations) {
        this.validations = validations;
    }

}
