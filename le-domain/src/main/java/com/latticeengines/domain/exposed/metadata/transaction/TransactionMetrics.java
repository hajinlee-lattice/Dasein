package com.latticeengines.domain.exposed.metadata.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum TransactionMetrics {

    PURCHASED("Purchased"), //
    QUANTITY("Quantity"), //
    AMOUNT("Amount");

    private static Map<String, TransactionMetrics> nameMap;

    static {
        nameMap = new HashMap<>();
        for (TransactionMetrics metrics : TransactionMetrics.values()) {
            nameMap.put(metrics.getName(), metrics);
        }
    }

    private final String name;

    TransactionMetrics(String name) {
        this.name = name;
    }

    public static Set<String> availableNames() {
        return new HashSet<>(nameMap.keySet());
    }

    public static TransactionMetrics fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a purchase metrics with name " + name);
        }
    }

    public static String getProductIdFromAttr(String attr) {
        return splitAttr(attr, 0);
    }

    public static String getPeriodFromAttr(String attr) {
        return splitAttr(attr, 1);
    }

    public static String getMetricFromAttr(String attr) {
        return splitAttr(attr, 2);
    }

    private static String splitAttr(String name, int index) {
        if (!name.startsWith("PH_")) {
            return null;
        }
        name = name.substring("PH_".length());
        String[] attrs = name.split("_");
        if (attrs.length != 3) {
            return null;
        }
        return attrs[index];
    }

    public static String getAttrName(String product, String period, String metrics) {
        return "PH_" + product + "_" + period + "_" + metrics;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
    }
}
