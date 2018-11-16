package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

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
    private boolean mappedToLatticeField;

    // Represents the date/time format string provided by the user for a date attributes.  Eg. "MM/DD/YYYY 00:00:00"
    @JsonProperty
    private String dateTimeFormatString;

    // Represents the timezone to interpret date/time values provided by the user for a date attribute.
    @JsonProperty
    private String timezone;

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

    public String getDateTimeFormatString() {
        return this.dateTimeFormatString;
    }

    public void setDateTimeFormatString(String dateTimeFormatString) {
        this.dateTimeFormatString = dateTimeFormatString;
    }

    public String getTimezone() { return timezone; }

    public void setTimezone(String timezone) { this.timezone = timezone; }

    public String toString() {
        return JsonUtils.serialize(this);
    }
}
