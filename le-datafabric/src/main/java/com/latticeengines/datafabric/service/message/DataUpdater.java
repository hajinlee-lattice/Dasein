package com.latticeengines.datafabric.service.message;

public interface DataUpdater<T extends Object> {
    T update(T origData);
}
