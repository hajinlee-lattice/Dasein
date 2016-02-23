package com.latticeengines.domain.exposed.mapreduce.counters;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;

public class Counters implements org.apache.hadoop.mapreduce.v2.api.records.Counters {

    @JsonSubTypes(value = { @Type(value = CounterGroup.class) })
    private List<CounterGroup> counterGroups = new ArrayList<>();

    private String job_Id;

    @JsonProperty("counterGroup")
    public List<CounterGroup> getCounterGroupList() {
        return counterGroups;
    }

    @JsonProperty("counterGroup")
    public void setCounterGroupList(List<CounterGroup> counterGroups) {
        this.counterGroups = counterGroups;
    }

    @JsonProperty("id")
    public String getJobId() {
        return job_Id;
    }

    @JsonProperty("id")
    public void setJobId(String job_Id) {
        this.job_Id = job_Id;
    }

    @Override
    @JsonIgnore
    public Map<String, org.apache.hadoop.mapreduce.v2.api.records.CounterGroup> getAllCounterGroups() {
        Map<String, org.apache.hadoop.mapreduce.v2.api.records.CounterGroup> map = new HashMap<String, org.apache.hadoop.mapreduce.v2.api.records.CounterGroup>();
        for (org.apache.hadoop.mapreduce.v2.api.records.CounterGroup counterGroup : counterGroups) {
            map.put(counterGroup.getName(), counterGroup);
        }
        return map;
    }

    @Override
    @JsonIgnore
    public org.apache.hadoop.mapreduce.v2.api.records.CounterGroup getCounterGroup(String key) {
        return getAllCounterGroups().get(key);
    }

    @Override
    @JsonIgnore
    public org.apache.hadoop.mapreduce.v2.api.records.Counter getCounter(Enum<?> key) {
        return getAllCounterGroups().get(key.getClass().getCanonicalName()).getCounter(key.name());
    }

    @Override
    @JsonIgnore
    public void addAllCounterGroups(Map<String, org.apache.hadoop.mapreduce.v2.api.records.CounterGroup> counterGroups) {
        counterGroups.putAll(counterGroups);
    }

    @Override
    @JsonIgnore
    public void setCounterGroup(String key, org.apache.hadoop.mapreduce.v2.api.records.CounterGroup value) {
        removeCounterGroup(key);
        counterGroups.add((CounterGroup) value);
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
        throw new UnsupportedOperationException();
    }

}
