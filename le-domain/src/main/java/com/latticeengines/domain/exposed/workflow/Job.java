package com.latticeengines.domain.exposed.workflow;

import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;

public class Job implements HasId<Long>, HasName {

    public static final EnumSet<JobStatus> TERMINAL_JOB_STATUS = EnumSet.of(JobStatus.COMPLETED, JobStatus.CANCELLED,
            JobStatus.FAILED);

    private Long id;
    private String name;
    private String description;
    private String applicationId;
    private Date startTimestamp;
    private Date endTimestamp;
    private JobStatus jobStatus;
    private String jobType;
    private String user;
    private List<JobStep> steps;
    private List<Report> reports;
    private Map<String, String> inputs;
    private Map<String, String> outputs;

    @Override
    @JsonProperty
    public Long getId() {
        return id;
    }

    @Override
    @JsonProperty
    public void setId(Long id) {
        this.id = id;
    }

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
    public String getJobType() {
        return jobType;
    }

    @JsonProperty
    public void setJobType(String jobType) {
        this.jobType = jobType;
    }

    @JsonProperty
    public String getUser() {
        return user;
    }

    @JsonProperty
    public void setUser(String user) {
        this.user = user;
    }

    @JsonProperty
    public List<JobStep> getSteps() {
        return steps;
    }

    @JsonProperty
    public void setSteps(List<JobStep> steps) {
        this.steps = steps;
    }

    @JsonProperty
    public List<Report> getReports() {
        return reports;
    }

    @JsonProperty
    public void setReports(List<Report> reports) {
        this.reports = reports;
    }

    @JsonProperty
    public Map<String, String> getInputs() {
        return inputs;
    }

    @JsonProperty
    public void setInputs(Map<String, String> inputs) {
        this.inputs = inputs;
    }

    @JsonProperty
    public Map<String, String> getOutputs() {
        return outputs;
    }

    @JsonProperty
    public void setOutputs(Map<String, String> outputs) {
        this.outputs = outputs;
    }

    @JsonProperty
    public String getApplicationId() {
        return applicationId;
    }

    @JsonProperty
    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
