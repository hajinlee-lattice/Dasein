package com.latticeengines.domain.exposed.datafabric.generic;

public enum GenericFabricStatus {
    FINISHED("Finished"), PROCESSING("Processing"), ERROR("Error"), UNKNOWN("Unknown");

    private String value;

    private GenericFabricStatus(String status) {
        this.value = status;
    }

    public static GenericFabricStatus typeOf(String status) {
        for (GenericFabricStatus enumStatus : GenericFabricStatus.values()) {
            if (enumStatus.value.equalsIgnoreCase(status)) {
                return enumStatus;
            }
        }
        return UNKNOWN;
    }
}
