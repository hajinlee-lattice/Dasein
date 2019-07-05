package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TemplateFieldPreview {

    @JsonProperty("name_from_file")
    private String nameFromFile;

    @JsonProperty("name_in_template")
    private String nameInTemplate;

    @JsonProperty("field_type")
    private UserDefinedType fieldType;

    @JsonProperty("date_format_string")
    private String dateFormatString;

    @JsonProperty("time_format_string")
    private String timeFormatString;

    @JsonProperty("time_zone")
    private String timezone;

    @JsonProperty("field_category")
    private FieldCategory fieldCategory;

    @JsonProperty("unmapped")
    private boolean unmapped = false;

    public String getNameFromFile() {
        return nameFromFile;
    }

    public void setNameFromFile(String nameFromFile) {
        this.nameFromFile = nameFromFile;
    }

    public String getNameInTemplate() {
        return nameInTemplate;
    }

    public void setNameInTemplate(String nameInTemplate) {
        this.nameInTemplate = nameInTemplate;
    }

    public UserDefinedType getFieldType() {
        return fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
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

    public FieldCategory getFieldCategory() {
        return fieldCategory;
    }

    public void setFieldCategory(FieldCategory fieldCategory) {
        this.fieldCategory = fieldCategory;
    }

    public boolean isUnmapped() {
        return unmapped;
    }

    public void setUnmapped(boolean unmapped) {
        this.unmapped = unmapped;
    }
}
