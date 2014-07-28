package com.latticeengines.domain.exposed.dataplatform;

public interface HasProperty {

    String getPropertyValue(String key);
    
    void setPropertyValue(String key, String value);
}
