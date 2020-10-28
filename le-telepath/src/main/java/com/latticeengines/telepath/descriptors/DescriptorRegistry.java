package com.latticeengines.telepath.descriptors;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.telepath.entities.Entity;

public final class DescriptorRegistry {

    private static final Logger log = LoggerFactory.getLogger(DescriptorRegistry.class);

    protected DescriptorRegistry() {
        throw new UnsupportedOperationException("Should not instantiate");
    }

    private static final Map<Class<? extends Entity>, BaseDescriptor<? extends Entity>> registry = new HashMap<>();

    public static <T extends Entity> void register(Class<T> entityClz, BaseDescriptor<T> descriptor) {
        if (!registry.containsKey(entityClz)) {
            registry.putIfAbsent(entityClz, descriptor);
            log.info("Registered a descriptor of type {} for dependency entity {}",
                    descriptor.getClass().getSimpleName(), entityClz.getSimpleName());
        }
    }

    public static <T extends Entity> BaseDescriptor<? extends Entity> getDescriptor(Class<T> entityClz) {
        if (!registry.containsKey(entityClz)) {
            throw new IllegalArgumentException(String.format("Descriptor for entity class %s is not found in registry.",
                    entityClz.getSimpleName()));
        }
        return registry.get(entityClz);
    }
}
