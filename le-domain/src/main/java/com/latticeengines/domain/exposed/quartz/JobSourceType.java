package com.latticeengines.domain.exposed.quartz;

public enum JobSourceType {
    DEFAULT(0), MANUAL(1);

    private int value;

    JobSourceType(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static JobSourceType valueOf(int value) {
        JobSourceType result = null;
        for (JobSourceType type : JobSourceType.values()) {
            if (value == type.getValue()) {
                result = type;
                break;
            }
        }
        return result;
    }
}
