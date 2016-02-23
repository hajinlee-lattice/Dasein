package com.latticeengines.domain.exposed.mapreduce.counters;

import org.apache.hadoop.mapreduce.v2.api.records.Counter;

import com.fasterxml.jackson.annotation.JsonProperty;

public class CounterImpl implements Counter {

    private String name;
    private String displayName;
    private long value;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("displayName")
    public String getDisplayName() {
        return displayName;
    }

    @Override
    @JsonProperty("value")
    public long getValue() {
        return value;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    @JsonProperty("displayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    @JsonProperty("value")
    public void setValue(long value) {
        this.value = value;
    }

}
