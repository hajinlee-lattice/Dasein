package com.latticeengines.domain.exposed.dataplatform.dlorchestration;

public enum ModelCommandStatus {
    NEW(0), IN_PROGRESS(2), SUCCESS(3), FAIL(4);

    private int value;

    ModelCommandStatus(int value) {
        this.value = value;
    }

    public static ModelCommandStatus valueOf(int value) {
        ModelCommandStatus result = null;
        for (ModelCommandStatus status : ModelCommandStatus.values()) {
            if (value == status.getValue()) {
                result = status;
                break;
            }
        }
        return result;
    }

    public int getValue() {
        return value;
    }

}
