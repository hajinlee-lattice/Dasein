package com.latticeengines.aws.batch;

import java.util.Map;

public class JobRequest {

    private Map<String, String> parameters;
    private String jobQueue = "AWS-Python-Workflow-Job-Queue";
    private String jobName;
    private String jobDefinition = "AWS-Python-Workflow-Job-Definition";
    private Integer cpus;
    private Integer memory;
    private Map<String, String> envs;

    public String getJobDefinition() {
        return this.jobDefinition;
    }

    public String getJobName() {
        return this.jobName;
    }

    public String getJobQueue() {
        return this.jobQueue;
    }

    public Map<String, String> getParameters() {
        return this.parameters;
    }

    public JobRequest setParameters(Map<String, String> parameters) {
        this.parameters = parameters;
        return this;
    }

    public JobRequest setJobQueue(String jobQueue) {
        this.jobQueue = jobQueue;
        return this;
    }

    public JobRequest setJobName(String jobName) {
        this.jobName = jobName;
        return this;
    }

    public JobRequest setJobDefinition(String jobDefinition) {
        this.jobDefinition = jobDefinition;
        return this;
    }

    public Integer getCpus() {
        return cpus;
    }

    public Integer getMemory() {
        return memory;
    }

    public void setMemory(Integer memory) {
        this.memory = memory;
    }

    public void setCpus(Integer cpus) {
        this.cpus = cpus;
    }

    public Map<String, String> getEnvs() {
        return envs;
    }

    public void setEnvs(Map<String, String> envs) {
        this.envs = envs;
    }

}
