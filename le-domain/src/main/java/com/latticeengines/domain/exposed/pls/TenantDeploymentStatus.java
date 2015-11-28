package com.latticeengines.domain.exposed.pls;

public enum TenantDeploymentStatus {
    NEW(0), IN_PROGRESS(1), SUCCESS(2), FAIL(3), WARNING(4);

    private int value;

    private TenantDeploymentStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TenantDeploymentStatus valueOf(int value) {
        TenantDeploymentStatus result = null;
        for (TenantDeploymentStatus status : TenantDeploymentStatus.values()) {
            if (value == status.getValue()) {
                result = status;
                break;
            }
        }
        return result;
    }
}
