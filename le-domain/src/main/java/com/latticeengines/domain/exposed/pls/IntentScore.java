package com.latticeengines.domain.exposed.pls;

public enum IntentScore {
    NONE(0), //
    LOW(1), //
    MEDIUM(2), //
    HIGH(3), //
    MAX(4);

    private final int value;

    IntentScore(final int value) {
        this.value = value;
    }

    public int getValue() {
        return value;
    }

    public static IntentScore fromInteger(int value) {
        for (IntentScore score : values()) {
            if (score.getValue() == value) {
                return score;
            }
        }
        throw new RuntimeException(String.format("No such IntentScore with value %s", value));
    }
}
