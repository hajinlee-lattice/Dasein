package com.latticeengines.dataplatform.exposed.domain;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class ThrottleConfiguration {

    private boolean immediate = false;
    private int jobRankCutoff;

    @JsonProperty("immediate")
    public boolean isImmediate() {
        return immediate;
    }

    @JsonProperty("immediate")
    public void setImmediate(boolean immediate) {
        this.immediate = immediate;
    }

    @JsonProperty("jobrank_cutoff")
    public int getJobRankCutoff() {
        return jobRankCutoff;
    }

    @JsonProperty("jobrank_cutoff")
    public void setJobRankCutoff(int jobRankCutoff) {
        this.jobRankCutoff = jobRankCutoff;
    }
    
    @Override
    public String toString() {
        return JsonHelper.serialize(this);
    }

 
}
