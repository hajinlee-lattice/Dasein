package com.latticeengines.domain.exposed.metadata;

public interface HasProperties<T extends MetadataProperty<?>> {

    T getProperty(String key);

    void putProperty(String key, String value);

}
