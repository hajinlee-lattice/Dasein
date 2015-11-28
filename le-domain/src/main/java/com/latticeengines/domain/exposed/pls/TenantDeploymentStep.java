package com.latticeengines.domain.exposed.pls;

public enum TenantDeploymentStep {
    ENTER_CREDENTIALS(0),
    IMPORT_SFDC_DATA(1),
    ENRICH_DATA(2),
    VALIDATE_METADATA(3),
    ADD_MISSING_METADATA(4),
    PREPARE_BUILD_OPTIONS(5),
    CONFIGURE_BUILD_OPTIONS(6),
    BUILD_MODEL(7),
    SCORE_LEADS(8),
    REVIEW_MODEL(9),
    PUBLISH_SCORES(10);

    private int value;

    private TenantDeploymentStep(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TenantDeploymentStep valueOf(int value) {
        TenantDeploymentStep result = null;
        for (TenantDeploymentStep status : TenantDeploymentStep.values()) {
            if (value == status.getValue()) {
                result = status;
                break;
            }
        }
        return result;
    }
}
