package com.latticeengines.domain.exposed.pls.frontend;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldValidationResult {

    @JsonProperty
    private List<FieldValidation> fieldValidations;

    @JsonProperty
    private String errorMessage;

    @JsonProperty
    private boolean exceedQuotaLimit;

    public boolean isExceedQuotaLimit() {
        return exceedQuotaLimit;
    }

    public void setExceedQuotaLimit(boolean exceedQuotaLimit) {
        this.exceedQuotaLimit = exceedQuotaLimit;
    }


    public List<FieldValidation> getFieldValidations() {
        return fieldValidations;
    }

    public void setFieldValidations(List<FieldValidation> fieldValidations) {
        this.fieldValidations = fieldValidations;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }
}
