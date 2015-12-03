package com.latticeengines.domain.exposed.workflow;

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
    private JobStatus stepStatus;
    private String jobStepType;


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
    public JobStatus getStepStatus() {
        return stepStatus;
    }

    @JsonProperty
    public void setStepStatus(JobStatus stepStatus) {
        this.stepStatus = stepStatus;
    }

    @JsonProperty
    public String getJobStepType() {
        return jobStepType;
    }

    @JsonProperty
    public void setJobStepType(String jobStepType) {
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
