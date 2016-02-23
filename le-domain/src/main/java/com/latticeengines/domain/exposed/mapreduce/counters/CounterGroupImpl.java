package com.latticeengines.domain.exposed.mapreduce.counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CounterGroupImpl implements CounterGroup {

    private String name;
    private String displayName;
    List<CounterImpl> counters = new ArrayList<>();

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

    @JsonProperty("counters")
    public List<CounterImpl> getCounterList() {
        return counters;
    }

    @JsonProperty("counters")
    public void setCounterList(List<CounterImpl> counters) {
        this.counters = counters;
    }

    @Override
    @JsonIgnore
    public Map<String, Counter> getAllCounters() {
        Map<String, Counter> map = new HashMap<String, Counter>();
        for (Counter counter : counters) {
            map.put(counter.getName(), counter);
        }
        return map;
    }

    @Override
    @JsonIgnore
    public Counter getCounter(String key) {
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
    public void addAllCounters(Map<String, Counter> counters) {
        counters.putAll(counters);
    }

    @Override
    @JsonIgnore
    public void setCounter(String key, Counter value) {
        removeCounter(key);
        counters.add((CounterImpl) value);
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
