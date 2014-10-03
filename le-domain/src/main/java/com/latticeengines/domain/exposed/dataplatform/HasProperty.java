package com.latticeengines.domain.exposed.dataplatform;

public interface HasProperty {

    Object getPropertyValue(String key);
    
    void setPropertyValue(String key, Object value);
}
