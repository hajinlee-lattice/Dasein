package com.latticeengines.telepath.entities;

import java.util.HashMap;
import java.util.Map;

public final class EntityRegistry {
    protected EntityRegistry() {
        throw new UnsupportedOperationException("Should not instantiate");
    }

    private static final Map<String, Class<? extends Entity>> registry = new HashMap<>();

    public static void register(String type, Class<? extends Entity> clz) {
        if (registry.containsKey(type) && registry.get(type) != clz) {
            throw new IllegalStateException(
                    String.format("There already is an entity class %s registered under type %s",
                            registry.get(type).getSimpleName(), type));
        }
        registry.putIfAbsent(type, clz);
    }

    public static Class<? extends Entity> getEntityClass(String type) {
        if (!registry.containsKey(type)) {
            throw new IllegalArgumentException(String.format("Entity class of type %s not found in registry", type));
        }
        return registry.get(type);
    }
}
