package com.latticeengines.domain.exposed.security;

public enum TenantEmailNotificationType {
    SINGLE_USER,
    ALL_USER;

    public static TenantEmailNotificationType getByName(String entity) {
        for (TenantEmailNotificationType tenantEmailNotificationType : values()) {
            if (tenantEmailNotificationType.name().equalsIgnoreCase(entity)) {
                return tenantEmailNotificationType;
            }
        }
        throw new IllegalArgumentException(
                String.format("There is no entity name %s in TenantEmailNotificationType", entity));
    }
}
