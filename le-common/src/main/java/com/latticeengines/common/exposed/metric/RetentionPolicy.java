package com.latticeengines.common.exposed.metric;

public interface RetentionPolicy {

    String getDuration();

    Integer getReplication();

    String getName();

}
