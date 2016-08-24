package com.latticeengines.domain.exposed.propdata.manage;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public enum Predefined implements Dimension {
    LeadEnrichment("LeadEnrichment"), //
    Enrichment("Enrichment"), //
    DerivedColumns("DerivedColumns"), //
    Model("Model"), //
    RTS("RTS");

    private final String name;
    private static Map<String, Predefined> nameMap;
    static {
        nameMap = new HashMap<>();
        for (Predefined predefined : Predefined.values()) {
            nameMap.put(predefined.getName(), predefined);
        }
    }

    Predefined(String name) {
        this.name = name;
    }

    @MetricTag(tag = "PredefinedSelection")
    public String getName() {
        return this.name;
    }

    public static Predefined fromName(String name) {
        return nameMap.get(name);
    }

    public String getJsonFileName(String version) {
        return getName() + "_" + version + ".json";
    }

    public static EnumSet<Predefined> supportedSelections = EnumSet.of(Model, DerivedColumns, RTS);

    public static Predefined getLegacyDefaultSelection() {
        return DerivedColumns;
    }

    public static Predefined getDefaultSelection() {
        return RTS;
    }

    public static List<String> getNames() {
        return new ArrayList<>(nameMap.keySet());
    }

}