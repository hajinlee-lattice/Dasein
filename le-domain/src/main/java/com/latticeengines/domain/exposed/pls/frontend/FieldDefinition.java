package com.latticeengines.domain.exposed.pls.frontend;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.metadata.FundamentalType;
import com.latticeengines.domain.exposed.metadata.LogicalDataType;
import com.latticeengines.domain.exposed.metadata.UserDefinedType;

// FieldDefinition defines the properties of each column to field mapping in the Import Workflow Redesign and
// replaces the FieldMapping class.
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FieldDefinition {

    //
    // Properties that are part of the API with the UI.
    //

    // Defined in Spec
    // Internal name of the schema field that columns are mapped to.  Must be unique in each template and unique
    // across all templates representing the same business entity.
    @JsonProperty
    protected String fieldName;

    // Defined in Spec
    // The data format of this schema field.
    @JsonProperty
    protected UserDefinedType fieldType;

    // Defined in Spec
    // The name to display on the UI for this field.  Only used for Lattice Fields and Match Fields.
    @JsonProperty
    protected String screenName;

    // Defined by autodetection on import CSV or an existing template.
    // The name of the mapped column in the imported files for this schema field.  Also know as display name in the
    // Attribute class.  This is the header name of each CSV column and can be configured by the user.
    @JsonProperty
    protected String columnName;

    // Defined by autodetection on import CSV.
    // True if this field is mapped to a column in the current import process.  Only provided in API request and
    // response bodies.
    @JsonProperty
    protected Boolean inCurrentImport;


    //
    // Properties that are part of the API with the UI, but only apply for fields with fieldType "DATE".
    // ie. Date Attributes.
    // These fields are defined by autodetection on import CSV or an existing template.
    //

    // Represents the date format string provided by the user.  Eg. "MM/DD/YYYY"
    @JsonProperty
    protected String dateFormat;

    // Represents the time format string provided by the user.  Eg. "00:00:00 24H"
    @JsonProperty
    protected String timeFormat;

    // Represents the time zone for date/time values provided by the user.  Eg. America/New_York.
    @JsonProperty
    protected String timeZone;


    //
    // Properties defined by the template specification (Spec) which may need to be propagated by the UI back to the
    // backend to be stored in the template (Attribute Metadata Table).  Some of these fields are used for
    // autodetection and validation while others are used for P&A and modeling.
    //

    // Defined in Spec
    // A priority ordered list of column names that should be autodetected to match to this field.
    @JsonProperty
    protected List<String> matchingColumnNames;

    // Defined in Spec
    // True if this field is required for this template.  Use for validation.
    @JsonProperty
    protected Boolean required;

    // TODO(jwinter): Figure out where there is an enum (ApprovedUsage) and a class (ModelingMetadata) with similar
    //     defined values for this field.
    // Defined in Spec
    // Defines the allowed modeling usage of this property.
    @JsonProperty
    protected List<String> approvedUsage;

    // TODO(jwinter): Can we somehow do away with this partially redundant data type?
    // Defined in Spec
    // The logical meaning of the data type format, used in modeling.
    @JsonProperty
    protected LogicalDataType logicalDataType;

    // TODO(jwinter): Can we somehow do away with this mostly redundant data type?
    // Defined in Spec
    // Yet another field describing the data type format, used in the P&A pipeline and modeling, and VisiDB and LPI.
    @JsonProperty
    protected FundamentalType fundamentalType;

    // Yet another field describing the data type format, used possibly only in VisiDB and LPI.
    // Defined in Spec
    @JsonProperty
    protected String statisticalType;

    // Field category for modeling.
    // Defined in Spec
    @JsonProperty
    protected String category;

    // Field subcategory for modeling.
    // Defined in Spec
    @JsonProperty
    protected String subcategory;


    //
    // Properties for defining template IDs and linking with external systems.  These are not set for most fields.
    //
    // TODO(jwinter): These field are probably better as FieldDefinitionsRecord fields rather than defining them for
    //     every field in the template when their usage is so sparse.

    // TODO(jwinter):  Need to better explain what this property represents.
    // Indicates whether this field should be treated as the global ID for this entity type, which represents the
    // backwards compatible "Entity Id", such as Account ID or Contact ID.
    @JsonProperty
    protected Boolean mappedToLatticeId;

    public enum IdEntityType {
        Account,
        Contact
    }

    // When this field represents an ID, this enumeration represents the entity type of the ID.  Currently, the only
    // supported values are Account and Contact.
    @JsonProperty
    protected IdEntityType idEntityType;

    // When this field represents an external system ID (as set on the Other IDs page), eg. a Salesforce Contact ID,
    // this property indicates the type of external system the ID is from.
    @JsonProperty
    protected CDLExternalSystemType externalSystemType = null;

    // When this field represents an external system ID, this property represents that name of that system.
    // eg. Salesforce Contacts.
    @JsonProperty
    protected String externalSystemName;


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

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public Boolean isInCurrentImport() {
        return inCurrentImport;
    }

    public void setInCurrentImport(Boolean inCurrentImport) {
        this.inCurrentImport = inCurrentImport;
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

    public Boolean isRequired() {
        return required;
    }

    public void setRequired(Boolean required) {
        this.required = required;
    }

    public List<String> getApprovedUsage() {
        return approvedUsage;
    }

    public void setApprovedUsage(List<String> approvedUsage) {
        this.approvedUsage = approvedUsage;
    }

    public LogicalDataType getLogicalDataType() {
        return logicalDataType;
    }

    public void setLogicalDataType(LogicalDataType logicalDataType) {
        this.logicalDataType = logicalDataType;
    }

    public FundamentalType getFundamentalType() {
        return fundamentalType;
    }

    public void setFundamentalType(FundamentalType fundamentalType) {
        this.fundamentalType = fundamentalType;
    }

    public String getStatisticalType() {
        return statisticalType;
    }

    public void setStatisticalType(String statisticalType) {
        this.statisticalType = statisticalType;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getSubcategory() {
        return subcategory;
    }

    public void setSubcategory(String subcategory) {
        this.subcategory = subcategory;
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

    @Override
    public String toString() {
        String output = "";
        output += "fieldName: " + fieldName;
        output += "\nfieldType: " + fieldType;
        output += "\nscreenName: " + screenName;
        output += "\ncolumnName: " + columnName;
        output += "\ninCurrentImport: " + inCurrentImport;
        output += "\ndateFormat: " + dateFormat;
        output += "\ntimeFormat: " + timeFormat;
        output += "\ntimeZone: " + timeZone;
        output += "\nmatchingColumnNames:";
        if (CollectionUtils.isNotEmpty(matchingColumnNames)) {
            output += String.join(" ", matchingColumnNames);
        }
        output += "\nrequired: " + required;
        output += "\napprovedUsage:";
        if (CollectionUtils.isNotEmpty(approvedUsage)) {
            output += String.join(" ", approvedUsage);
        }
        output += "\nlogicalDataType: " + logicalDataType;
        output += "\nfundamentalType: " + fundamentalType;
        output += "\nstatisticalType: " + statisticalType;
        output += "\ncategory: " + category;
        output += "\nsubcategory: " + subcategory;
        output += "\nmappedToLatticeId: " + mappedToLatticeId;
        output += "\nidEntityType: " + idEntityType;
        output += "\nexternalSystemType: " + externalSystemType;
        output += "\nexternalSystemName: " + externalSystemName;
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
                    !StringUtils.equals(this.dateFormat, definition.dateFormat) ||
                    !StringUtils.equals(this.timeFormat, definition.timeFormat) ||
                    !StringUtils.equals(this.timeZone, definition.timeZone) ||
                    this.logicalDataType != definition.logicalDataType ||
                    this.fundamentalType != definition.fundamentalType ||
                    !StringUtils.equals(this.statisticalType, definition.statisticalType) ||
                    !StringUtils.equals(this.category, definition.category) ||
                    !StringUtils.equals(this.subcategory, definition.subcategory) ||
                    this.idEntityType != definition.idEntityType ||
                    this.externalSystemType != definition.externalSystemType ||
                    !StringUtils.equals(this.externalSystemName, definition.externalSystemName)) {
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
            if (this.approvedUsage == null) {
                if (definition.approvedUsage != null) {
                    return false;
                }
            } else if (!this.approvedUsage.equals(definition.approvedUsage)) {
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
