package com.latticeengines.domain.exposed.serviceapps.core;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.collections4.MapUtils;

public class ValidationDetails {

    @JsonProperty("validations")
    private List<AttrValidation> validations;

    public List<AttrValidation> getValidations() {
        return validations;
    }

    public void setValidations(List<AttrValidation> validations) {
        this.validations = validations;
    }

    @JsonIgnore
    public void addValidation(AttrValidation validation) {
        if (validations == null) {
            validations = new ArrayList<>();
        }
        validations.add(validation);
    }

    public boolean hasError() {
        boolean hasError = false;
        if (CollectionUtils.isNotEmpty(validations)) {
            hasError = validations.stream().anyMatch(validation -> //
            validation.validationErrors != null && MapUtils.isNotEmpty(validation.validationErrors.getErrors()));
        }
        return hasError;
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class AttrValidation {

        @JsonProperty("attr_name")
        private String attrName;

        @JsonProperty("validation_error")
        private ValidationErrors validationErrors;

        @JsonProperty("impact_warning")
        private ImpactWarnings impactWarnings;

        public String getAttrName() {
            return attrName;
        }

        public void setAttrName(String attrName) {
            this.attrName = attrName;
        }

        public ValidationErrors getValidationErrors() {
            return validationErrors;
        }

        public void setValidationErrors(ValidationErrors validationErrors) {
            this.validationErrors = validationErrors;
        }

        public ImpactWarnings getImpactWarnings() {
            return impactWarnings;
        }

        public void setImpactWarnings(ImpactWarnings impactWarnings) {
            this.impactWarnings = impactWarnings;
        }
    }
}
