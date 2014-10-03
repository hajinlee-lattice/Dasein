package com.latticeengines.domain.exposed.sparkdb;

public interface Function<T> {
    
    T apply(Object... params);

}
