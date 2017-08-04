package com.latticeengines.domain.exposed.pls;

public enum EntityExternalType {

    Account, //
    Contact, //
    Opportunity, //
    Engagement, //
    Product, //
    Activity;

    public static EntityExternalType getByName(String entity) {
        for (EntityExternalType type : values()) {
            if (type.name().equalsIgnoreCase(entity)) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.format("There is no entity name %s in EntityExternalType", entity));
    }
}
