package com.latticeengines.domain.exposed.workflow;

public class JobCache {
    private Job job;
    private Long updatedAt;

    public JobCache() {
    }

    public JobCache(Job job, Long updatedAt) {
        this.job = job;
        this.updatedAt = updatedAt;
    }

    public Job getJob() {
        return job;
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
