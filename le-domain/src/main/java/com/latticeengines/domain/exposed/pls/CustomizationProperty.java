package com.latticeengines.domain.exposed.pls;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;

import com.fasterxml.jackson.annotation.JsonProperty;

@MappedSuperclass
public class CustomizationProperty {

    @JsonProperty("property_name")
    @Column(name = "PROPERTY_NAME", nullable = false)
    private String propertyName;

    @JsonProperty("property_value")
    @Column(name = "PROPERTY_VALUE")
    private String propertyValue;

    public void setPropertyName(String propertyNameString) {
        CustomizationPropertyName propertyName = CustomizationPropertyName.fromName(propertyNameString);
        if (propertyName == null) {
            throw new RuntimeException(String.format("No such CustomizationProperty %s", propertyNameString));
        }
        this.propertyName = propertyNameString;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public void setPropertyValue(String propertyValue) {
        this.propertyValue = propertyValue;
    }

    public String getPropertyValue() {
        return propertyValue;
    }
}
