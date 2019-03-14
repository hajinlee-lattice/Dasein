package com.latticeengines.domain.exposed.metadata.transaction;

public enum ProductType {
    Bundle, Hierarchy, Analytic, Spending, Raw;

    public static ProductType getProductType(String name) {
        for (ProductType type : values()) {
            if (type.name().equals(name)) {
                return type;
            }
        }
        return Raw;
    }
}
