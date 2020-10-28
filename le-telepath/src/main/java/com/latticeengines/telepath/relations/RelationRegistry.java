package com.latticeengines.telepath.relations;

import java.util.HashMap;
import java.util.Map;

public final class RelationRegistry {
    protected RelationRegistry() {
        throw new UnsupportedOperationException("Should not instantiate");
    }

    public static final Map<String, Class<? extends BaseRelation>> registry = new HashMap<>();

    public static void register(String type, Class<? extends BaseRelation> clz) {
        if (registry.containsKey(type) && registry.get(type) != clz) {
            throw new IllegalStateException(
                    String.format("There already is a relation class %s registered under type %s",
                            registry.get(type).getSimpleName(), type));
        }
        registry.putIfAbsent(type, clz);
    }

    public static Class<? extends BaseRelation> getRelationClass(String type) {
        if (!registry.containsKey(type)) {
            throw new IllegalArgumentException(String.format("Relation class of type %s not found in registry", type));
        }
        return registry.get(type);
    }
}
