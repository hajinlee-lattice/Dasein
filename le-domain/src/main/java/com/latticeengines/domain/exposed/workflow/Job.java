package com.latticeengines.domain.exposed.workflow;

import java.util.ArrayList;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import javax.persistence.Transient;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasId;
import com.latticeengines.domain.exposed.dataplatform.HasName;
import com.latticeengines.domain.exposed.exception.LedpCode;

public class Job implements HasId<Long>, HasName {

    public static final EnumSet<JobStatus> TERMINAL_JOB_STATUS = EnumSet.of(JobStatus.COMPLETED,
            JobStatus.CANCELLED, JobStatus.FAILED, JobStatus.SKIPPED);
    public static final EnumSet<JobStatus> NON_TERMINAL_JOB_STATUS = EnumSet.complementOf(TERMINAL_JOB_STATUS);

    private Long pid;
    private Long id;
    private String name;
    private Long parentId;
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
    private LedpCode errorCode;
    private String errorMsg;
    private Integer numDisplayedSteps;
    private String note;
    private List<Job> subJobs;
    private Long tenantPid;
    private String tenantId;
    private String errorCategory;
    private SchedulingInfo schedulingInfo;

    @JsonProperty
    public Long getPid() {
        return pid;
    }

    @JsonProperty
    public void setPid(Long pid) {
        this.pid = pid;
    }

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
    public Long getParentId() {
        return parentId;
    }

    @JsonProperty
    public void setParentId(Long parentId) {
        this.parentId = parentId;
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

    @JsonProperty
    public LedpCode getErrorCode() {
        return errorCode;
    }

    @JsonProperty
    public void setErrorCode(LedpCode errorCode) {
        this.errorCode = errorCode;
    }

    @JsonProperty
    public String getErrorMsg() {
        return errorMsg;
    }

    @JsonProperty
    public void setErrorMsg(String errorMsg) {
        this.errorMsg = errorMsg;
    }

    @JsonProperty
    public Integer getNumDisplayedSteps() {
        return numDisplayedSteps;
    }

    @JsonProperty
    public void setNumDisplayedSteps(Integer numDisplayedSteps) {
        this.numDisplayedSteps = numDisplayedSteps;
    }

    @JsonProperty("note")
    public String getNote() {
        return this.note;
    }

    @JsonProperty("note")
    public void setNote(String note) {
        this.note = note;
    }

    @JsonProperty("subJobs")
    public List<Job> getSubJobs() {
        return this.subJobs;
    }

    @JsonProperty("subJobs")
    public void setSubJobs(List<Job> subJobs) {
        this.subJobs = subJobs;
    }

    public void addSubJobs(Job job) {
        if (this.subJobs == null) {
            this.subJobs = new ArrayList<>();
        }
        this.subJobs.add(job);
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public Long getTenantPid() {
        return tenantPid;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public void setTenantPid(Long tenantPid) {
        this.tenantPid = tenantPid;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public String getTenantId() {
        return tenantId;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Transient
    @JsonIgnore
    public boolean isRunning() {
        return !TERMINAL_JOB_STATUS.contains(getJobStatus());
    }

    @JsonProperty
    public String getErrorCategory() {
        return errorCategory;
    }

    @JsonProperty
    public void setErrorCategory(String errorCategory) {
        this.errorCategory = errorCategory;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public SchedulingInfo getSchedulingInfo() {
        return schedulingInfo;
    }

    @JsonProperty
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public void setSchedulingInfo(SchedulingInfo schedulingInfo) {
        this.schedulingInfo = schedulingInfo;
    }

    public Job shallowClone() {
        Job job = new Job();
        job.pid = this.pid;
        job.id = this.id;
        job.name = this.name;
        job.parentId = this.parentId;
        job.description = this.description;
        job.applicationId = this.applicationId;
        job.startTimestamp = this.startTimestamp;
        job.endTimestamp = this.endTimestamp;
        job.jobStatus = this.jobStatus;
        job.jobType = this.jobType;
        job.user = this.user;
        job.steps = this.steps;
        job.reports = this.reports;
        job.inputs = this.inputs;
        job.outputs = this.outputs;
        job.errorCode = this.errorCode;
        job.errorMsg = this.errorMsg;
        job.numDisplayedSteps = this.numDisplayedSteps;
        job.note = this.note;
        job.subJobs = this.subJobs;
        job.tenantPid = this.tenantPid;
        job.tenantId = this.tenantId;
        job.errorCategory = this.errorCategory;
        job.schedulingInfo = this.schedulingInfo;
        return job;
    }

    /*
     * scheduler information for this job and its tenant
     */
    public static class SchedulingInfo {
        private final boolean schedulerEnabled;
        private final boolean scheduled;

        public SchedulingInfo(boolean schedulerEnabled, boolean scheduled) {
            this.schedulerEnabled = schedulerEnabled;
            this.scheduled = scheduled;
        }

        public boolean isSchedulerEnabled() {
            return schedulerEnabled;
        }

        public boolean isScheduled() {
            return scheduled;
        }
    }
}
