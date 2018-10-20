package com.latticeengines.domain.exposed.dante.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.FundamentalType;

public class PropertyMetadata extends BaseObjectMetadata {
    @JsonProperty("DefaultValue")
    private String defaultValue;

    @JsonProperty("Interpretation")
    private int interpretation;

    @JsonProperty("InterpretationString")
    private String interpretationString;

    @JsonProperty("Nullable")
    private boolean nullable;

    @JsonProperty("PropertyType")
    private int propertyType;

    @JsonProperty("PropertyTypeString")
    private String propertyTypeString;

    @JsonProperty("Required")
    private boolean required;

    @JsonProperty("TargetNotion")
    private String targetNotion;

    @JsonProperty("PropertyAggregationString")
    private String propertyAggregationString;

    public PropertyMetadata() {
    }

    public PropertyMetadata(ColumnMetadata columnMetadata) {
        interpretation = 0;
        interpretationString = "Value";
        setSource(MetadataSource.DataCloud);
        nullable = true;
        setName(columnMetadata.getAttrName());
        setDisplayNameKey(columnMetadata.getDisplayName());
        setDescriptionKey(columnMetadata.getDescription());
        setPropertyType(convertFundamentalTypeToPropertyType(columnMetadata.getFundamentalType())
                .getPropertyTypeNumber());
        setPropertyTypeString(
                convertFundamentalTypeToPropertyType(columnMetadata.getFundamentalType())
                        .toString());
    }

    public String getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(String defaultValue) {
        this.defaultValue = defaultValue;
    }

    public int getInterpretation() {
        return interpretation;
    }

    public void setInterpretation(int interpretation) {
        this.interpretation = interpretation;
    }

    public String getInterpretationString() {
        return interpretationString;
    }

    public void setInterpretationString(String interpretationString) {
        this.interpretationString = interpretationString;
    }

    public boolean isNullable() {
        return nullable;
    }

    public void setNullable(boolean nullable) {
        this.nullable = nullable;
    }

    public int getPropertyType() {
        return propertyType;
    }

    public void setPropertyType(int propertyType) {
        this.propertyType = propertyType;
    }

    public String getPropertyTypeString() {
        return propertyTypeString;
    }

    public void setPropertyTypeString(String propertyTypeString) {
        this.propertyTypeString = propertyTypeString;
    }

    public boolean isRequired() {
        return required;
    }

    public void setRequired(boolean required) {
        this.required = required;
    }

    public String getTargetNotion() {
        return targetNotion;
    }

    public void setTargetNotion(String targetNotion) {
        this.targetNotion = targetNotion;
    }

    public String getPropertyAggregationString() {
        return propertyAggregationString;
    }

    public void setPropertyAggregationString(String propertyAggregationString) {
        this.propertyAggregationString = propertyAggregationString;
    }

    private PropertyType convertFundamentalTypeToPropertyType(FundamentalType value) {
        switch (value) {
            case ALPHA:
            case EMAIL:
            case URI:
            case PHONE:
                return PropertyType.String;

            case BOOLEAN:
                return PropertyType.Bool;

            case CURRENCY:
                return PropertyType.Currency;

            case DATE:
            case YEAR:
                return PropertyType.DateTime;

            case PERCENTAGE:
                return PropertyType.Percentage;

            case NUMERIC:
                return PropertyType.Decimal;

            case ENUM:
            default:
                return PropertyType.String;
        }
    }
}
