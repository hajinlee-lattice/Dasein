package com.latticeengines.domain.exposed.dataplatform;

public interface HasPid extends HasPidTemplated<Long> {

    Long getPid();

    void setPid(Long pid);

}
