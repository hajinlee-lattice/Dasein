package com.latticeengines.domain.exposed.query;

public enum ComparisonType {
    IS_NULL, //
    IS_NOT_NULL, //
    EQUAL, //
    NOT_EQUAL, //
    GREATER_THAN, //
    GREATER_OR_EQUAL, //
    LESS_THAN, //
    LESS_OR_EQUAL, //
    IN_COLLECTION, //
    NOT_IN_COLLECTION, //
    CONTAINS, //
    NOT_CONTAINS, //
    STARTS_WITH, //
    ENDS_WITH, //
    GTE_AND_LTE, //
    GTE_AND_LT, //
    GT_AND_LTE, //
    GT_AND_LT, //
    EVER, //
    IN_CURRENT_PERIOD, //
    BETWEEN, //
    PRIOR_ONLY, //
    FOLLOWING, //
    WITHIN, //
    BEFORE, // only for exact date
    AFTER, // only for exact date
    BETWEEN_DATE, // only for exact date range

    PRIOR, // only for internal use, do not expose to UI
    WITHIN_INCLUDE; // only for internal use, do not expose to UI

    public boolean isLikeTypeOfComparison() {
        return this == CONTAINS || this == NOT_CONTAINS || this == STARTS_WITH || this == ENDS_WITH;
    }

    public boolean filter(String source, String target) {
        switch (this) {
            case CONTAINS:
                // currently rely on negate to find complement of contains
            case NOT_CONTAINS:
                return source.toLowerCase().contains(target.toLowerCase());
            case STARTS_WITH:
                return source.toLowerCase().startsWith(target.toLowerCase());
            case ENDS_WITH:
                return source.toLowerCase().endsWith(target.toLowerCase());
            default:
                throw new UnsupportedOperationException();
        }
    }
}
