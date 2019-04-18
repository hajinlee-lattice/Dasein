package com.latticeengines.domain.exposed.quartz;

public enum TriggeredJobStatus {
    SUCCESS(0), FAIL(1), START(2), TIMEOUT(3), TRIGGERED(4);

    private int value;

    TriggeredJobStatus(int value) {
        this.value = value;
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

    public int getValue() {
        return value;
    }

}
