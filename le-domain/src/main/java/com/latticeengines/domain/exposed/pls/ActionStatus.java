package com.latticeengines.domain.exposed.pls;

public enum ActionStatus {

    ACTIVE,
    CANCELED;

    public static ActionStatus getByName(String entity) {
        for (ActionStatus actionStatus : values()) {
            if (actionStatus.name().equalsIgnoreCase(entity)) {
                return actionStatus;
            }
        }
        throw new IllegalArgumentException(
                String.format("There is no entity name %s in ActionStatus", entity));
    }
}
