package com.latticeengines.domain.exposed.metadata;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum Tag {
    INTERNAL("Internal"), //
    EXTERNAL("External"), //
    INTERNAL_TRANSFORM("InternalTransform"), //
    EXTERNAL_TRANSFORM("ExternalTransform");

    private final String name;
    private static Map<String, Tag> nameMap;

    static {
        nameMap = new HashMap<>();
        for (Tag tag : Tag.values()) {
            nameMap.put(tag.getName(), tag);
        }
    }

    Tag(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    public static Set<String> availableNames() {
        return new HashSet<>(nameMap.keySet());
    }

    public static Tag fromName(String name) {
        if (nameMap.containsKey(name)) {
            return nameMap.get(name);
        } else {
            throw new IllegalArgumentException("Cannot find a Tag with name " + name);
        }
    }

}
