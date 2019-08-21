package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

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

    // The name to display on the UI for this field.  Only used for Lattice Fields and Match Fields.
    @JsonProperty
    private String screenName;

    // A priority ordered list of column names that should be matched to this field.  Only set in the Import Workflow
    // Specs.
    @JsonProperty
    private List<String> matchingColumnNames;

    // The name of the mapped column in the imported files for this schema field.  Also know as display name.
    @JsonProperty
    private String columnName;

    // True if this field is required for this template.
    @JsonProperty
    Boolean required;

    // True if this field is mapped to a column in the current import process.  Only provided in API request and
    // response bodies.
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

    public String getScreenName() {
        return screenName;
    }

    public void setScreenName(String screenName) {
        this.screenName = screenName;
    }

    public List<String> getMatchingColumnNames() {
        return matchingColumnNames;
    }

    public void setMatchingColumnNames(List<String> matchingColumnNames) {
        this.matchingColumnNames = matchingColumnNames;
    }

    public void addMatchingColumnNames(String columnName) {
        if (matchingColumnNames == null) {
            matchingColumnNames = new ArrayList<>();
        }
        if (!matchingColumnNames.contains(columnName)) {
            matchingColumnNames.add(columnName);
        }
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
        output += "\nscreenName: " + screenName;
        output += "\nmatchingColumnNames:";
        if (CollectionUtils.isNotEmpty(matchingColumnNames)) {
            for (String name : matchingColumnNames) {
                output += " " + name;
            }
        }
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

    @Override
    public boolean equals(Object object) {
        if (object instanceof FieldDefinition) {
            FieldDefinition definition = (FieldDefinition) object;
            if (!StringUtils.equals(this.fieldName, definition.fieldName) ||
                    this.fieldType != definition.fieldType ||
                    !StringUtils.equals(this.screenName, definition.screenName) ||
                    !StringUtils.equals(this.columnName, definition.columnName) ||
                    this.idEntityType != definition.idEntityType ||
                    this.externalSystemType != definition.externalSystemType ||
                    !StringUtils.equals(this.externalSystemName, definition.externalSystemName) ||
                    !StringUtils.equals(this.dateFormat, definition.dateFormat) ||
                    !StringUtils.equals(this.timeFormat, definition.timeFormat) ||
                    !StringUtils.equals(this.timeZone, definition.timeZone)) {
                return false;
            }

            if (this.matchingColumnNames == null) {
                if (definition.matchingColumnNames != null) {
                    return false;
                }
            } else if (!this.matchingColumnNames.equals(definition.matchingColumnNames)) {
                return false;
            }
            if (this.required == null) {
                if (definition.required != null) {
                    return false;
                }
            } else if (!this.required.equals(definition.required)) {
                return false;
            }
            if (this.inCurrentImport == null) {
                if (definition.inCurrentImport != null) {
                    return false;
                }
            } else if (!this.inCurrentImport.equals(definition.inCurrentImport)) {
                return false;
            }
            if (this.mappedToLatticeId == null) {
                if (definition.mappedToLatticeId != null) {
                    return false;
                }
            } else if (!this.mappedToLatticeId.equals(definition.mappedToLatticeId)) {
                return false;
            }
            return true;
        }
        return false;
    }
}
