package com.latticeengines.domain.exposed.security;

public enum TenantEmailNotificationLevel {

    NONE,
    ERROR,
    WARNING,
    INFO;

    public static TenantEmailNotificationLevel getByName(String entity) {
        for (TenantEmailNotificationLevel tenantEmailNotificationLevel : values()) {
            if (tenantEmailNotificationLevel.name().equalsIgnoreCase(entity)) {
                return tenantEmailNotificationLevel;
            }
        }
        throw new IllegalArgumentException(
                String.format("There is no entity name %s in TenantEmailNotificationLevel", entity));
    }
}
