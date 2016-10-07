package com.latticeengines.domain.exposed.metadata;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum ApprovedUsage {

    MODEL("Model"),
    MODEL_ALLINSIGHTS("ModelAndAllInsights"),
    MODEL_MODELINSIGHTS("ModelAndModelInsights"),
    ENRICHMENT("Enrichment"),
    NONE("None");

    private final String name;
    private static Map<String, ApprovedUsage> nameMap;
    private static final EnumSet<ApprovedUsage> MODELING_APPROVEDUSAGE = EnumSet.of(MODEL, MODEL_ALLINSIGHTS,
            MODEL_MODELINSIGHTS);

    static {
        nameMap = new HashMap<>();
        for (ApprovedUsage approvedUsage: ApprovedUsage.values()) {
            nameMap.put(approvedUsage.getName(), approvedUsage);
        }
    }

    ApprovedUsage(String name) {
        this.name = name;
    }

    public String getName() { return this.name; }

    public String toString() { return this.name; }

    public static ApprovedUsage fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else  {
            return ApprovedUsage.NONE;
        }
    }

    public static boolean isUsedByModeling(String name){
        return MODELING_APPROVEDUSAGE.contains(ApprovedUsage.fromName(name));
    }
}
