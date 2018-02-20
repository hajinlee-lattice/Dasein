package com.latticeengines.domain.exposed.metadata.transaction;

public enum ProductType {
    ANALYTIC("Analytic"), //
    SPENDING("Spending"), //
    RAW("Raw");

    private final String name;

    ProductType(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
    }
}
