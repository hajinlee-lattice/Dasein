package com.latticeengines.dataplatform.exposed.domain;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;

public class ThrottleConfiguration implements HasId<Long>{

    private Boolean immediate = Boolean.FALSE;
    private Boolean enabled = Boolean.TRUE;
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
        return JsonUtils.serialize(this);
    }

    @Override
    @JsonIgnore
    public Long getId() {
        return id;
    }

    @Override
    @JsonIgnore
    public void setId(Long id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @JsonProperty("enabled")
    public Boolean isEnabled() {
        return enabled;
    }

    @JsonProperty("enabled")
    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

 
}
