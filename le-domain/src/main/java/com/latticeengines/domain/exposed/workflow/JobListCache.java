package com.latticeengines.domain.exposed.workflow;

import java.util.List;

public class JobListCache {
    private List<Job> jobs;
    private Long updatedAt;

    public List<Job> getJobs() {
        return jobs;
    }

    public void setJobs(List<Job> jobs) {
        this.jobs = jobs;
    }

    public Long getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(Long updatedAt) {
        this.updatedAt = updatedAt;
    }
}
