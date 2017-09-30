package com.latticeengines.domain.exposed.metadata.transaction;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum TransactionMetrics {
    PURCHASED("Purchased"), //
    QUANTITY("Quantity"), //
    AMOUNT("Amount");

    private final String name;

    private static Map<String, TransactionMetrics> nameMap;

    static {
        nameMap = new HashMap<>();
        for (TransactionMetrics metrics : TransactionMetrics.values()) {
            nameMap.put(metrics.getName(), metrics);
        }
    }

    TransactionMetrics(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
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


    static public String getProductIdFromAttr(String attr) {
        return splitAttr(attr, 0);
    }

    static public String getPeriodFromAttr(String attr) {
        return splitAttr(attr, 1);
    }

    static public String getMetricFromAttr(String attr) {
        return splitAttr(attr, 2);
    }

    static private String splitAttr(String name, int index) {
        String[] attrs = name.split("_");
        if ((attrs.length != 4) || !(attrs[0].equals("PH"))) {
            return null;
        } else {
            return attrs[index];
        }
    }

    static public String getAttrName(String product, String period, String metrics) {
        return "PH_" + product + "_" + period + "_" + metrics;
    }
}
