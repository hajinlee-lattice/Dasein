package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldValidation {

    @JsonProperty
    private String userField;

    @JsonProperty
    private String latticeField;

    @JsonProperty
    private ValidationStatus status;

    @JsonProperty
    private String message;

    public String getUserField() {
        return userField;
    }

    public void setUserField(String userField) {
        this.userField = userField;
    }

    public String getLatticeField() {
        return latticeField;
    }

    public void setLatticeField(String latticeField) {
        this.latticeField = latticeField;
    }

    public ValidationStatus getStatus() {
        return status;
    }

    public void setStatus(ValidationStatus status) {
        this.status = status;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public enum ValidationStatus {
        VALID, WARNING, ERROR, IGNORED, INFO
    }
}
