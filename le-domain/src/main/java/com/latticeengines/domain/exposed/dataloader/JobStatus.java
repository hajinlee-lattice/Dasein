package com.latticeengines.domain.exposed.dataloader;

public enum JobStatus {
    In_Queue(0), IN_PROGRESS(1), SUCCESS(2), FAIL(3);

    private int value;

    private JobStatus(int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static JobStatus valueOf(int value) {
        JobStatus result = null;
        for (JobStatus status : JobStatus.values()) {
            if (value == status.getValue()) {
                result = status;
                break;
            }
        }
        return result;
    }
}
