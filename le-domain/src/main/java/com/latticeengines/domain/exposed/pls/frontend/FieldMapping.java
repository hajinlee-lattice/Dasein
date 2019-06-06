package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldMapping {

    @JsonProperty
    private String userField;

    @JsonProperty
    private String mappedField;

    @JsonProperty
    private UserDefinedType fieldType;

    @JsonProperty
    private CDLExternalSystemType cdlExternalSystemType = null;

    @JsonProperty
    private String systemName;

    /**
     * for System id type, should be one of (Account, Contact)
     */
    @JsonProperty
    private IdType idType;

    @JsonProperty
    private boolean mappedToLatticeField;

    // Represents the date format string provided by the user for a date attributes.  Eg. "MM/DD/YYYY"
    @JsonProperty
    private String dateFormatString;

    // Represents the time format string provided by the user for a date attributes.  Eg. "00:00:00 24H"
    @JsonProperty
    private String timeFormatString;

    // Represents the timezone to interpret date/time values provided by the user for a date attribute.
    @JsonProperty
    private String timezone;

    // Judge the field was mapped to date or time format before
    @JsonProperty
    private boolean mappedToDateBefore;

    public String getUserField() {
        return this.userField;
    }

    public void setUserField(String userField) {
        this.userField = userField;
    }

    public String getMappedField() {
        return this.mappedField;
    }

    public void setMappedField(String mappedField) {
        this.mappedField = mappedField;
    }

    public UserDefinedType getFieldType() {
        return this.fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public CDLExternalSystemType getCdlExternalSystemType() {
        return cdlExternalSystemType;
    }

    public void setCdlExternalSystemType(CDLExternalSystemType cdlExternalSystemType) {
        this.cdlExternalSystemType = cdlExternalSystemType;
    }

    public boolean isMappedToLatticeField() {
        return this.mappedToLatticeField;
    }

    public void setMappedToLatticeField(boolean mappedToLatticeField) {
        this.mappedToLatticeField = mappedToLatticeField;
    }

    public String getDateFormatString() {
        return this.dateFormatString;
    }

    public void setDateFormatString(String dateFormatString) {
        this.dateFormatString = dateFormatString;
    }

    public String getTimeFormatString() {
        return this.timeFormatString;
    }

    public void setTimeFormatString(String timeFormatString) {
        this.timeFormatString = timeFormatString;
    }

    public boolean isMappedToDateBefore() {
        return mappedToDateBefore;
    }

    public void setMappedToDateBefore(boolean mappedToDateBefore) {
        this.mappedToDateBefore = mappedToDateBefore;
    }

    public String getTimezone() { return timezone; }

    public void setTimezone(String timezone) { this.timezone = timezone; }

    public String getSystemName() {
        return systemName;
    }

    public void setSystemName(String systemName) {
        this.systemName = systemName;
    }

    public IdType getIdType() {
        return idType;
    }

    public void setIdType(IdType idType) {
        this.idType = idType;
    }

    public String toString() {
        return JsonUtils.serialize(this);
    }

    public enum IdType {
        Account, Contact
    }
}
