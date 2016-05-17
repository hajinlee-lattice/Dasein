package com.latticeengines.domain.exposed.transform;

import java.util.HashMap;
import java.util.Map;

public enum TransformationGroup {

    STANDARD("standard"), //
    POC("poc"), //
    ALL("all"); //

    private final String name;
    private static Map<String, TransformationGroup> nameMap;

    static {
        nameMap = new HashMap<>();
        for (TransformationGroup transformationGroup : TransformationGroup.values()) {
            nameMap.put(transformationGroup.getName(), transformationGroup);
        }
    }

    TransformationGroup(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public String toString() {
        return this.name;
    }
}
