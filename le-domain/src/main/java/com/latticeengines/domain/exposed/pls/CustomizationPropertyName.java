package com.latticeengines.domain.exposed.pls;

public enum CustomizationPropertyName {
    hidden("hidden", Boolean.class), //
    highlighted("highlighted", Boolean.class);

    private final String name;
    private final Class type;

    CustomizationPropertyName(String name, Class type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public Class getType() {
        return type;
    }

    public static CustomizationPropertyName fromName(String propertyNameString) {
        for (CustomizationPropertyName propertyName : CustomizationPropertyName.values()) {
            if (propertyName.getName().equals(propertyNameString)) {
                return propertyName;
            }
        }
        return null;
    }
}
