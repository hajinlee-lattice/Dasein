package com.latticeengines.skald;

public enum ScoreType {
    PROBABILITY(Double.class),

    LIFT(Double.class),

    PERCENTILE(Integer.class),

    BUCKET(String.class);

    private ScoreType(Class<?> type) {
        this.type = type;
    }

    private final Class<?> type;

    public Class<?> type() {
        return type;
    }
}