package com.latticeengines.domain.exposed.dataplatform;

public interface HasPidTemplated<T> {

    T getPid();

    void setPid(T pid);
}
