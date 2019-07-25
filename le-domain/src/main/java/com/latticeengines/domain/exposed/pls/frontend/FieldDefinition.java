package com.latticeengines.domain.exposed.pls.frontend;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDefinition {

    //
    // Basic properties that all FieldDefinitions should have.
    //

    // Internal name of the schema field that columns are mapped to.  Must be unique for each mapping.
    @JsonProperty
    private String fieldName;

    // The data format of this schema
    @JsonProperty
    private UserDefinedType fieldType;

    // The name of the mapped column in the imported files for this schema field.  Also know as display name.
    @JsonProperty
    private String columnName;

    // True if this field is required for this template.
    @JsonProperty
    Boolean required;

    // True if this field is mapped to a column in the current import process.
    @JsonProperty
    Boolean inCurrentImport;

    //
    // These properties are specialized metadata properties that are only required for some FieldDefinitions
    //

    // TODO(jwinter):  Need to better explain what this property represents.
    // Indicates whether this field should be treated as the global ID for this entity type, which represents the
    // backwards compatible "Entity Id", such as Account ID or Contact ID.
    @JsonProperty
    private Boolean mappedToLatticeId;

    public enum IdEntityType {
        Account,
        Contact
    }

    // When this field represents an ID, this enumeration represents the entity type of the ID.  Currently, the only
    // supported values are Account and Contact.
    @JsonProperty
    private IdEntityType idEntityType;

    // When this field represents an external system ID (as set on the Other IDs page), eg. a Salesforce Contact ID,
    // this property indicates the type of external system the ID is from.
    @JsonProperty
    private CDLExternalSystemType externalSystemType = null;

    // When this field represents an external system ID, this property represents that name of that system.
    // eg. Salesforce Contacts.
    @JsonProperty
    private String externalSystemName;

    //
    // The properties only apply for fields with fieldType "DATE", ie. Date Attributes.
    //

    // Represents the date format string provided by the user.  Eg. "MM/DD/YYYY"
    @JsonProperty
    private String dateFormat;

    // Represents the time format string provided by the user.  Eg. "00:00:00 24H"
    @JsonProperty
    private String timeFormat;

    // Represents the time zone for date/time values provided by the user.  Eg. America/New_York.
    @JsonProperty
    private String timeZone;

    //
    // Getters and setters.
    //

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public UserDefinedType getFieldType() {
        return fieldType;
    }

    public void setFieldType(UserDefinedType fieldType) {
        this.fieldType = fieldType;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Boolean isRequired() {
        return required;
    }

    public void setRequired(Boolean required) {
        this.required = required;
    }

    public Boolean isInCurrentImport() {
        return inCurrentImport;
    }

    public void setInCurrentImport(Boolean inCurrentImport) {
        this.inCurrentImport = inCurrentImport;
    }

    public Boolean isMappedToLatticeId() {
        return mappedToLatticeId;
    }

    public void setMappedToLatticeId(Boolean mappedToLatticeId) {
        this.mappedToLatticeId = mappedToLatticeId;
    }

    public IdEntityType getIdEntityType() {
        return idEntityType;
    }

    public void setIdEntityType(IdEntityType idEntityType) {
        this.idEntityType = idEntityType;
    }

    public CDLExternalSystemType getExternalSystemType() {
        return externalSystemType;
    }

    public void setExternalSystemType(CDLExternalSystemType externalSystemType) {
        this.externalSystemType = externalSystemType;
    }

    public String getExternalSystemName() {
        return externalSystemName;
    }

    public void setExternalSystemName(String externalSystemName) {
        this.externalSystemName = externalSystemName;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getTimeFormat() {
        return timeFormat;
    }

    public void setTimeFormat(String timeFormat) {
        this.timeFormat = timeFormat;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    @Override
    public String toString() {
        String output = "";
        output += "fieldName: " + fieldName;
        output += "\nfieldType: " + fieldType;
        output += "\ncolumnName: " + columnName;
        output += "\nrequired: " + required;
        output += "\ninCurrentImport: " + inCurrentImport;
        output += "\nmappedToLatticeId: " + mappedToLatticeId;
        output += "\nidEntityType: " + idEntityType;
        output += "\nexternalSystemType: " + externalSystemType;
        output += "\nexternalSystemName: " + externalSystemName;
        output += "\ndateFormat: " + dateFormat;
        output += "\ntimeFormat: " + timeFormat;
        output += "\ntimeZone: " + timeZone;
        return output;
    }
}
