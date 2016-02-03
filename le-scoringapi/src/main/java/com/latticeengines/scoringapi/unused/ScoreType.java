package com.latticeengines.scoringapi.unused;

/**
 * Output field of a scoring operation.
 */
public enum ScoreType {

    PROBABILITY(Double.class),

    LIFT(Double.class),

    PERCENTILE(Integer.class),

    BUCKET(String.class),

    MODEL_NAME(String.class);

    private ScoreType(Class<?> type) {
        this.type = type;
    }

    private final Class<?> type;

    public Class<?> type() {
        return type;
    }

    public static Object parse(ScoreType scoretype, String value) {
        if (value == null) {
            return null;
        }

        Class<?> type = scoretype.type();
        try {
            if (type == Double.class) {
                return Double.parseDouble(value);
            } else if (type == Integer.class) {
                return Integer.parseInt(value);
            } else if (type == String.class) {
                return value;
            } else {
                throw new RuntimeException("");
            }
        } catch (NumberFormatException e) {
            throw new RuntimeException("Unable to parse " + value + " as " + type);
        }
    }
}