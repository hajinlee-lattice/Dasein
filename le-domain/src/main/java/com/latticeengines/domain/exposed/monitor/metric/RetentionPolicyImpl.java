package com.latticeengines.domain.exposed.monitor.metric;

import com.latticeengines.common.exposed.metric.RetentionPolicy;

public enum RetentionPolicyImpl implements RetentionPolicy {
    DEFAULT("default", "INF", 1), ONE_WEEK("one-week", "1w", 1), ONE_HOUR("one-hour", "1h", 1);

    private String name;
    private String duration;
    private Integer replication;

    RetentionPolicyImpl(String name, String duration, Integer replication) {
        this.name = name;
        this.duration = duration;
        this.replication = replication;
    }

    @Override
    public String getDuration() {
        return duration;
    }

    @Override
    public Integer getReplication() {
        return replication;
    }

    @Override
    public String getName() {
        return name;
    }

}
