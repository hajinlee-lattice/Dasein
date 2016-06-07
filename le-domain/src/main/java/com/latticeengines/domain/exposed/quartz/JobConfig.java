package com.latticeengines.domain.exposed.quartz;

import com.fasterxml.jackson.annotation.JsonProperty;

public class JobConfig {

    @JsonProperty("job_name")
    private String jobName;

    @JsonProperty("cron_trigger")
    private String cronTrigger;

    @JsonProperty("dest_url")
    private String destUrl;

    @JsonProperty("query_api")
    private String queryApi;

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

    public String getQueryApi() {
        return queryApi;
    }

    public void setQueryApi(String queryApi) {
        this.queryApi = queryApi;
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
