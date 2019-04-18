package com.latticeengines.domain.exposed.scoring;

public enum ScoringCommandStatus {
    NEW(0), POPULATED(1), CONSUMED(2), SUCCESS(3), FAIL(4);

    private int value;

    ScoringCommandStatus(int value) {
        this.value = value;
    }

    public static ScoringCommandStatus valueOf(int value) {
        ScoringCommandStatus result = null;
        for (ScoringCommandStatus status : ScoringCommandStatus.values()) {
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
