package com.latticeengines.domain.exposed.elasticsearch;

public enum EsEntityType {
    VIData, Account, Contact, TimelineProfile;

    public static EsEntityType getByName(String entity) {
        for (EsEntityType type : values()) {
            if (type.name().equalsIgnoreCase(entity)) {
                return type;
            }
        }
        throw new IllegalArgumentException(String.format("There is no entity name %s in EsEntityType", entity));
    }
}
