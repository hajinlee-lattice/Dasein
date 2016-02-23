package com.latticeengines.domain.exposed.mapreduce.counters;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobCounters {

    private Counters counters;

    @JsonProperty("jobCounters")
    public Counters getCounters(){
        return counters;
    }

    @JsonProperty("jobCounters")
    public void setCounters(Counters counters){
        this.counters = counters;
    }
}
