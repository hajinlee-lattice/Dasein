package com.latticeengines.domain.exposed.cdl;

import java.util.HashMap;
import java.util.Map;

public enum CDLObjectTypes {
    Segment("Segment"), //
    Model("Model"), // i.e. RatingEngine
    Play("Play"); //

    private String objectType;
    private static Map<String, CDLObjectTypes> map = new HashMap<>();

    static {
        for (CDLObjectTypes r : CDLObjectTypes.values()) {
            map.put(r.getObjectType(), r);
        }
    }

    CDLObjectTypes(String objectType) {
        this.objectType = objectType;
    }

    public String getObjectType() {
        return objectType;
    }

    public static CDLObjectTypes getObjectTypeEnum(String objectType) {
        return map.get(objectType);
    }
}
