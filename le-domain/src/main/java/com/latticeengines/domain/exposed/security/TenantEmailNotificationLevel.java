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

    public static TenantEmailNotificationLevel getNameByNum(int status) {
        switch (status) {
            case 0 : return TenantEmailNotificationLevel.NONE;
            case 1 : return TenantEmailNotificationLevel.ERROR;
            case 3 : return TenantEmailNotificationLevel.WARNING;
            case 7 : return TenantEmailNotificationLevel.INFO;
        }
        throw new IllegalArgumentException(
                String.format("There is no entity status %s in TenantEmailNotificationLevel", status));
    }

    public static int getNotificationState(TenantEmailNotificationLevel tenantEmailNotificationLevel) {
        switch (tenantEmailNotificationLevel) {
            case NONE: return 0;
            case ERROR: return 1;
            case WARNING: return 3;
            case INFO: return 7;
        }
        throw new IllegalArgumentException(
                String.format("There is no valid state %s in TenantEmailNotificationLevel",
                        tenantEmailNotificationLevel.name()));
    }
}
