package com.latticeengines.domain.exposed.quartz;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobConfig {

    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("cron_trigger")
    private String cronTrigger;

    @JsonProperty("dest_url")
    private String destUrl;

    @JsonProperty("secondary_dest_url")
    private String secondaryDestUrl;

    @JsonProperty("query_api")
    private String queryApi;

    @JsonProperty("check_job_bean_url")
    private String checkJobBeanUrl;

    @JsonProperty("job_arguments")
    private String jobArguments;

    @JsonProperty("job_timeout")
    private int jobTimeout;

    public JobConfig() {
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public String getCronTrigger() {
        return cronTrigger;
    }

    public void setCronTrigger(String crontrigger) {
        this.cronTrigger = crontrigger;
    }

    public String getDestUrl() {
        return destUrl;
    }

    public void setDestUrl(String destUrl) {
        this.destUrl = destUrl;
    }

    public String getSecondaryDestUrl() {
        return secondaryDestUrl;
    }

    public void setSecondaryDestUrl(String secondaryDestUrl) {
        this.secondaryDestUrl = secondaryDestUrl;
    }

    public String getQueryApi() {
        return queryApi;
    }

    public void setQueryApi(String queryApi) {
        this.queryApi = queryApi;
    }

    public String getCheckJobBeanUrl() {
        return checkJobBeanUrl;
    }

    public void setCheckJobBeanUrl(String checkJobBeanUrl) {
        this.checkJobBeanUrl = checkJobBeanUrl;
    }

    public String getJobArguments() {
        return jobArguments;
    }

    public void setJobArguments(String jobArguments) {
        this.jobArguments = jobArguments;
    }

    public int getJobTimeout() {
        return jobTimeout;
    }

    public void setJobTimeout(int jobTimeout) {
        this.jobTimeout = jobTimeout;
    }

}
