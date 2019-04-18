package com.latticeengines.domain.exposed.datafabric.generic;

public enum GenericFabricStatusEnum {
    FINISHED("Finished"), PROCESSING("Processing"), ERROR("Error"), UNKNOWN("Unknown");

    private String value;

    GenericFabricStatusEnum(String status) {
        this.value = status;
    }

    public static GenericFabricStatusEnum typeOf(String status) {
        for (GenericFabricStatusEnum enumStatus : GenericFabricStatusEnum.values()) {
            if (enumStatus.value.equalsIgnoreCase(status)) {
                return enumStatus;
            }
        }
        return UNKNOWN;
    }
}
