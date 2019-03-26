package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class LatticeSchemaField {

    @JsonProperty
    private String name;

    @JsonProperty
    private RequiredType requiredType;

    @JsonProperty
    private UserDefinedType fieldType;

    @JsonProperty
    private String requiredIfNoField;

    @JsonProperty
    private Boolean fromExistingTemplate = false;

    @JsonProperty
    private String dateFormatString;

    @JsonProperty
    private String timeFormatString;

    @JsonProperty
    private String timezone;

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public RequiredType getRequiredType() {
        return this.requiredType;
    }

    public void setRequiredType(RequiredType requiredType) {
        this.requiredType = requiredType;
    }

    public UserDefinedType getFieldType() {
        return this.fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public String getRequiredIfNoField() {
        return this.requiredIfNoField;
    }

    public void setRequiredIfNoField(String requiredIfNoField) {
        this.requiredIfNoField = requiredIfNoField;
    }

    public Boolean getFromExistingTemplate() {
        return fromExistingTemplate;
    }

    public void setFromExistingTemplate(Boolean fromExistingTemplate) {
        this.fromExistingTemplate = fromExistingTemplate;
    }

    public String getDateFormatString() {
        return dateFormatString;
    }

    public void setDateFormatString(String dateFormatString) {
        this.dateFormatString = dateFormatString;
    }

    public String getTimeFormatString() {
        return timeFormatString;
    }

    public void setTimeFormatString(String timeFormatString) {
        this.timeFormatString = timeFormatString;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }
}
