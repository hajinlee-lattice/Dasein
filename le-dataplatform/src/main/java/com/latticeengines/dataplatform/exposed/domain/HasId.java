package com.latticeengines.dataplatform.exposed.domain;

public interface HasId<T> {

    T getId();
    
    void setId(T id);
    
}
