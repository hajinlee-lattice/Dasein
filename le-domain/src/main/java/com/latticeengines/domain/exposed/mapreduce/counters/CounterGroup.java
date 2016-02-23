package com.latticeengines.domain.exposed.mapreduce.counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CounterGroup implements org.apache.hadoop.mapreduce.v2.api.records.CounterGroup {

    private String name;
    private String displayName;
    List<Counter> counters = new ArrayList<>();

    @Override
    @JsonProperty("counterGroupName")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("counterGroupDisplayName")
    public String getDisplayName() {
        return displayName;
    }

    @JsonProperty("counter")
    public List<Counter> getCounterList() {
        return counters;
    }

    @JsonProperty("counter")
    public void setCounterList(List<Counter> counters) {
        this.counters = counters;
    }

    @Override
    @JsonIgnore
    public Map<String, org.apache.hadoop.mapreduce.v2.api.records.Counter> getAllCounters() {
        Map<String, org.apache.hadoop.mapreduce.v2.api.records.Counter> map = new HashMap<>();
        for (org.apache.hadoop.mapreduce.v2.api.records.Counter counter : counters) {
            map.put(counter.getName(), counter);
        }
        return map;
    }

    @Override
    @JsonIgnore
    public org.apache.hadoop.mapreduce.v2.api.records.Counter getCounter(String key) {
        return getAllCounters().get(key);
    }

    @Override
    @JsonProperty("counterGroupName")
    public void setName(String name) {
        this.name = name;
    }

    @Override
    @JsonProperty("counterGroupDisplayName")
    public void setDisplayName(String displayName) {
        this.displayName = displayName;
    }

    @Override
    @JsonIgnore
    public void addAllCounters(Map<String, org.apache.hadoop.mapreduce.v2.api.records.Counter> counters) {
        counters.putAll(counters);
    }

    @Override
    @JsonIgnore
    public void setCounter(String key, org.apache.hadoop.mapreduce.v2.api.records.Counter value) {
        removeCounter(key);
        counters.add((Counter) value);
    }

    @Override
    @JsonIgnore
    public void removeCounter(String key) {
        counters.remove(getAllCounters().get(key));
    }

    @Override
    @JsonIgnore
    public void clearCounters() {
        counters.clear();
    }

}
