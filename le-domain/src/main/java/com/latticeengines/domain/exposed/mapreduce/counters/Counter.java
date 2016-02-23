package com.latticeengines.domain.exposed.mapreduce.counters;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Counter implements org.apache.hadoop.mapreduce.v2.api.records.Counter {

    private String name;
    private String displayName;
    private long totalCounterValue;
    private long mapCounterValue;
    private long reduceCounterValue;

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
    @JsonProperty("totalCounterValue")
    public long getValue() {
        return totalCounterValue;
    }

    @JsonProperty("mapCounterValue")
    public long getMapValue() {
        return mapCounterValue;
    }

    @JsonProperty("reduceCounterValue")
    public long getReduceValue() {
        return reduceCounterValue;
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
    @JsonProperty("totalCounterValue")
    public void setValue(long value) {
        this.totalCounterValue = value;
    }
    
    @JsonProperty("mapCounterValue")
    public void setMapValue(long mapCounterValue) {
        this.mapCounterValue = mapCounterValue;
    }
    
    @JsonProperty("reduceCounterValue")
    public void setReduceValue(long reduceCounterValue) {
        this.reduceCounterValue = reduceCounterValue;
    }

}
