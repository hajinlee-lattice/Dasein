package com.latticeengines.domain.exposed.dataplatform;

import java.sql.Timestamp;

import javax.persistence.Basic;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.Transient;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;

@Entity
@Table(name = "THROTTLE_CONFIGURATION")
public class ThrottleConfiguration implements HasPid {

    private Boolean immediate = Boolean.FALSE;
    private Boolean enabled = Boolean.TRUE;
    private Integer jobRankCutoff;
    private Long pid;
    private Timestamp timestamp;

    public ThrottleConfiguration() {

    }

    public ThrottleConfiguration(Long pid) {
        this.pid = pid;
    }

    @Override
    @JsonIgnore
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Basic(optional = false)
    @Column(name = "PID", unique = true, nullable = false)
    public Long getPid() {
        return pid;
    }

    @Override
    @JsonIgnore
    public void setPid(Long id) {
        this.pid = id;
    }

    @JsonProperty("immediate")
    @Column(name = "IMMEDIATE")
    public boolean isImmediate() {
        return immediate;
    }

    @JsonProperty("immediate")
    public void setImmediate(Boolean immediate) {
        this.immediate = immediate;
    }

    @JsonProperty("jobrank_cutoff")
    @Column(name = "JOB_RANK_CUTOFF")
    public Integer getJobRankCutoff() {
        return jobRankCutoff;
    }

    @JsonProperty("jobrank_cutoff")
    public void setJobRankCutoff(Integer jobRankCutoff) {
        this.jobRankCutoff = jobRankCutoff;
    }

    @Column(name = "TIMESTAMP")
    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp ts) {
        this.timestamp = ts;
    }

    @Transient
    public Long getTimestampLong() {
        if (this.timestamp == null)
            return null;

        return this.timestamp.getTime();
    }

    public void setTimestampLong(Long timestamp) {
        if (timestamp != null) {
            this.timestamp = new Timestamp(timestamp);
        }
    }

    @JsonProperty("enabled")
    @Column(name = "ENABLED")
    public Boolean isEnabled() {
        return enabled;
    }

    @JsonProperty("enabled")
    public void setEnabled(Boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
