package com.latticeengines.proxy.exposed.quartz;

import java.util.Date;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.PropertyUtils;
import com.latticeengines.domain.exposed.quartz.JobConfig;
import com.latticeengines.domain.exposed.quartz.JobInfo;
import com.latticeengines.domain.exposed.quartz.JobInfoDetail;
import com.latticeengines.network.exposed.quartz.QuartzSchedulerInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component
public class QuartzSchedulerProxy extends BaseRestApiProxy implements QuartzSchedulerInterface {

    public QuartzSchedulerProxy() {
        super(PropertyUtils.getProperty("common.microservice.url"), "quartz/scheduler");
    }

    @Override
    public Boolean setSchedulerStatus(String status) {
        String url = constructUrl("/status?status={status}", status);
        return post("setSchedulerStatus", url, null, Boolean.class);
    }

    @Override
    public Boolean addJob(String tenantId, JobConfig jobConfig) {
        String url = constructUrl("/jobs/{tenantId}", tenantId);
        return post("addJob", url, jobConfig, Boolean.class);
    }

    @Override
    public Boolean deleteJob(String tenantId, String jobName) {
        String url = constructUrl("/jobs/{tenantId}/{jobName}", tenantId, jobName);
        delete("deleteJob", url);
        return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<JobInfo> listJobs(String tenantId) {
        String url = constructUrl("/jobs/{tenantId}", tenantId);
        return get("listJobs", url, List.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<JobInfo> listAllJobs() {
        String url = constructUrl("/jobs");
        return get("listJobs", url, List.class);
    }

    @Override
    public JobInfoDetail getJobDetail(String tenantId, String jobName) {
        String url = constructUrl("/jobs/{tenantId}/{jobName}", tenantId, jobName);
        return get("getJobDetails", url, JobInfoDetail.class);
    }

    @Override
    public Date getNextDateFromCronExpression(String cronExpression) {
        String url = constructUrl("/nextDate/{cronExpression}", cronExpression);
        return get("nextDate", url, Date.class);
    }

}
