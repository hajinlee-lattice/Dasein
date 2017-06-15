package com.latticeengines.domain.exposed.metadata;

public interface HasProperties<T extends MetadataProperty> {

    MetadataProperty getProperty(String key);

    void putProperty(String key, String value);

}
