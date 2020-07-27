package com.latticeengines.proxy.exposed.quartz;

import java.util.List;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class QuartzSchedulerProxy extends BaseRestApiProxy {

    public QuartzSchedulerProxy() {
        super(PropertyUtils.getProperty("common.microservice.url"), "quartz/scheduler");
    }

    public Boolean setSchedulerStatus(String status) {
        String url = constructUrl("/status?status={status}", status);
        return post("setSchedulerStatus", url, null, Boolean.class);
    }

    public Boolean addJob(String tenantId, JobConfig jobConfig) {
        String url = constructUrl("/jobs/{tenantId}", tenantId);
        return post("addJob", url, jobConfig, Boolean.class);
    }

    public Boolean deleteJob(String tenantId, String jobName) {
        String url = constructUrl("/jobs/{tenantId}/{jobName}", tenantId, jobName);
        delete("deleteJob", url);
        return true;
    }

    public List<JobInfo> listJobs(String tenantId) {
        String url = constructUrl("/jobs/{tenantId}", tenantId);
        return getList("listJobs", url, JobInfo.class);
    }

    public List<JobInfo> listAllJobs() {
        String url = constructUrl("/jobs");
        return getList("listJobs", url, JobInfo.class);
    }

    public JobInfoDetail getJobDetail(String tenantId, String jobName) {
        String url = constructUrl("/jobs/{tenantId}/{jobName}", tenantId, jobName);
        return get("getJobDetails", url, JobInfoDetail.class);
    }

}
