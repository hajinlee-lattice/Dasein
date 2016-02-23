package com.latticeengines.domain.exposed.mapreduce.counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.mapreduce.v2.api.records.Counter;
import org.apache.hadoop.mapreduce.v2.api.records.CounterGroup;
import org.apache.hadoop.mapreduce.v2.api.records.Counters;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class CountersImpl implements Counters {

    private List<CounterGroupImpl> counterGroups = new ArrayList<>();

    @JsonProperty("jobCounters")
    public List<CounterGroupImpl> getCounterGroupList() {
        return counterGroups;
    }

    @JsonProperty("jobCounters")
    public void setCounterGroupList(List<CounterGroupImpl> counterGroups) {
        this.counterGroups = counterGroups;
    }

    @Override
    @JsonIgnore
    public Map<String, CounterGroup> getAllCounterGroups() {
        Map<String, CounterGroup> map = new HashMap<String, CounterGroup>();
        for (CounterGroup counterGroup : counterGroups) {
            map.put(counterGroup.getName(), counterGroup);
        }
        return map;
    }

    @Override
    @JsonIgnore
    public CounterGroup getCounterGroup(String key) {
        return getAllCounterGroups().get(key);
    }

    @Override
    @JsonIgnore
    public Counter getCounter(Enum<?> key) {
        return getAllCounterGroups().get(key.getClass().getCanonicalName()).getCounter(key.name());
    }

    @Override
    @JsonIgnore
    public void addAllCounterGroups(Map<String, CounterGroup> counterGroups) {
        counterGroups.putAll(counterGroups);
    }

    @Override
    @JsonIgnore
    public void setCounterGroup(String key, CounterGroup value) {
        removeCounterGroup(key);
        counterGroups.add((CounterGroupImpl) value);
    }

    @Override
    @JsonIgnore
    public void removeCounterGroup(String key) {
        counterGroups.remove(key);
    }

    @Override
    @JsonIgnore
    public void clearCounterGroups() {
        counterGroups.clear();
    }

    @Override
    @JsonIgnore
    public void incrCounter(Enum<?> key, long amount) {
        long original = getAllCounterGroups().get(key.getClass().getCanonicalName()).getCounter(key.name()).getValue();
        getAllCounterGroups().get(key.getClass().getCanonicalName()).getCounter(key.name()).setValue(original + amount);
    }

}
