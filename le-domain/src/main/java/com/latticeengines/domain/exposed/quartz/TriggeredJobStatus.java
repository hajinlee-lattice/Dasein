package com.latticeengines.domain.exposed.quartz;

public enum TriggeredJobStatus {
    SUCCESS(0), FAIL(1), START(2), TIMEOUT(3);

    private int value;

    private TriggeredJobStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static TriggeredJobStatus valueOf(int value) {
        TriggeredJobStatus result = null;
        for (TriggeredJobStatus status : TriggeredJobStatus.values()) {
            if (value == status.getValue()) {
                result = status;
                break;
            }
        }
        return result;
    }
}
