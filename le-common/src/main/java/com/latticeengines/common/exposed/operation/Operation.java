package com.latticeengines.common.exposed.operation;

public interface Operation<T> {
    void perform(T parameter);
}
