package com.latticeengines.domain.exposed.metadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.metadata.validators.RequiredIfOtherFieldIsEmpty;

public class AttributeBuilder {

    private Attribute attribute = new Attribute();

    public AttributeBuilder() {
    }

    public Attribute build() {
        return attribute;
    }

    // For setting Attribute fields.
    public AttributeBuilder name(String name) {
        attribute.setName(name);
        return this;
    }

    public AttributeBuilder displayName(String displayName) {
        attribute.setDisplayName(displayName);
        return this;
    }

    public AttributeBuilder secondaryDisplayName(String secondaryDisplayName) {
        attribute.setSecondaryDisplayName(secondaryDisplayName);
        return this;
    }

    public AttributeBuilder length(Integer length) {
        attribute.setLength(length);
        return this;
    }

    public AttributeBuilder nullable(Boolean nullable) {
        attribute.setNullable(nullable);
        return this;
    }

    public AttributeBuilder notNull() {
        attribute.setNullable(false);
        return this;
    }

    public AttributeBuilder physicalDataType(Schema.Type type) {
        attribute.setPhysicalDataType(type.toString());
        return this;
    }

    public AttributeBuilder sourceLogicalDataType(String sourceLogicalDataType) {
        attribute.setSourceLogicalDataType(sourceLogicalDataType);
        return this;
    }

    public AttributeBuilder logicalType(LogicalDataType logicalDataType) {
        attribute.setLogicalDataType(logicalDataType);
        return this;
    }

    // For setting property bag values.

    // TODO(jinyang): Can we change this to take a List as parameter rather than convert from
    //   Set to List?
    public AttributeBuilder allowedDisplayNames(Set<String> allowedDisplayNames) {
        if (allowedDisplayNames != null) {
            List<String> list = new ArrayList<>(allowedDisplayNames);
            attribute.setAllowedDisplayNames(list);
        }
        return this;
    }

    public AttributeBuilder allowedDisplayNames(List<String> allowedDisplayNames) {
        attribute.setAllowedDisplayNames(allowedDisplayNames);
        return this;
    }


    public AttributeBuilder approvedUsage(String approvedUsage) {
        attribute.setApprovedUsage(approvedUsage);
        return this;
    }

    public AttributeBuilder category(String category) {
        attribute.setCategory(category);
        return this;
    }

    public AttributeBuilder subcategory(String subcategory) {
        attribute.setSubcategory(subcategory);
        return this;
    }

    public AttributeBuilder dateFormatString(String dateFormatString) {
        attribute.setDateFormatString(dateFormatString);
        return this;
    }

    public AttributeBuilder timeFormatString(String timeFormatString) {
        attribute.setTimeFormatString(timeFormatString);
        return this;
    }

    public AttributeBuilder timezone(String timezone) {
        attribute.setTimezone(timezone);
        return this;
    }

    public AttributeBuilder defaultValueStr(String defaultValueStr) {
        attribute.setDefaultValueStr(defaultValueStr);
        return this;
    }

    public AttributeBuilder fundamentalType(String fundamentalType) {
        attribute.setFundamentalType(fundamentalType);
        return this;
    }


    public AttributeBuilder interfaceName(InterfaceName interfaceName) {
        attribute.setInterfaceName(interfaceName);
        return this;
    }

    @SuppressWarnings("unused")
    public AttributeBuilder withValidator(String otherField) {
        attribute.addValidator(new RequiredIfOtherFieldIsEmpty(otherField));
        return this;
    }

    public AttributeBuilder required() {
        attribute.setRequired(true);
        return this;
    }

    public AttributeBuilder required(Boolean required) {
        attribute.setRequired(required);
        return this;
    }

    public AttributeBuilder statisticalType(String statisticalType) {
        attribute.setStatisticalType(statisticalType);
        return this;
    }

    public AttributeBuilder tag(String tag) {
        attribute.setTags(tag);
        return this;
    }
}
