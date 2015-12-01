package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class JobStep implements HasId<Long>, HasName {

    private Long id;
    private String name;
    private String description;
    private Date startTimestamp;
    private Date endTimestamp;
    private JobStatus jobStatus;
    private JobStepType jobStepType;


    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty
    public String getDescription() {
        return this.description;
    }

    @JsonProperty
    public void setDescription(String description) {
        this.description = description;
    }

    @JsonProperty
    public Date getStartTimestamp() {
        return startTimestamp;
    }

    @JsonProperty
    public void setStartTimestamp(Date startTimestamp) {
        this.startTimestamp = startTimestamp;
    }

    @JsonProperty
    public Date getEndTimestamp() {
        return endTimestamp;
    }

    @JsonProperty
    public void setEndTimestamp(Date endTimestamp) {
        this.endTimestamp = endTimestamp;
    }

    @JsonProperty
    public JobStatus getJobStatus() {
        return jobStatus;
    }

    @JsonProperty
    public void setJobStatus(JobStatus jobStatus) {
        this.jobStatus = jobStatus;
    }

    @JsonProperty
    public JobStepType getJobStepType() {
        return jobStepType;
    }

    @JsonProperty
    public void setJobStepType(JobStepType jobStepType) {
        this.jobStepType = jobStepType;
    }

    @JsonIgnore
    @Override
    public Long getId() {
        return id;
    }

    @Override
    @JsonIgnore
    public void setId(Long id) {
        this.id = id;
    }

}
