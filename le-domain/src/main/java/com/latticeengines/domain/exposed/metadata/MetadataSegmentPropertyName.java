package com.latticeengines.domain.exposed.metadata;

public enum MetadataSegmentPropertyName {
    NumAccounts("NumAccounts", Integer.class), //
    NumContacts("NumContacts", Integer.class), //
    Lift("Lift", Double.class);

    private String name;
    private Class<?> type;

    MetadataSegmentPropertyName(String name, Class<?> type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return this.name;
    }

    public Class<?> getType() {
        return this.type;
    }
}
