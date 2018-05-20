package com.latticeengines.domain.exposed.dante.metadata;

public enum PropertyType {
    Int("0"), //
    Int64("1"), //
    Decimal("2"), //
    Double("3"), //
    Bool("4"), //
    String("5"), //
    DateTimeOffset("6"), //
    Currency("7"), //
    Probability("8"), //
    Percentage("9"), //
    EpochTime("10"), //
    DateTime("11"), //
    Real("12"), //
    Segment("13"); //

    private int propertyType;

    PropertyType(String propertyTypeNum) {
        propertyType = Integer.parseInt(propertyTypeNum);
    }

    public int getPropertyTypeNumber() {
        return propertyType;
    }
}
