package com.latticeengines.dataplatform.exposed.domain;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class ThrottleConfiguration implements HasId<Long>{

    private Boolean immediate = false;
    private Integer jobRankCutoff;
    private Long id;
    private Long timestamp;

    @JsonProperty("immediate")
    public boolean isImmediate() {
        return immediate;
    }

    @JsonProperty("immediate")
    public void setImmediate(Boolean immediate) {
        this.immediate = immediate;
    }

    @JsonProperty("jobrank_cutoff")
    public Integer getJobRankCutoff() {
        return jobRankCutoff;
    }

    @JsonProperty("jobrank_cutoff")
    public void setJobRankCutoff(Integer jobRankCutoff) {
        this.jobRankCutoff = jobRankCutoff;
    }
    
    @Override
    public String toString() {
        return JsonHelper.serialize(this);
    }

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

 
}
