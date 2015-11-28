package com.latticeengines.domain.exposed.dataloader;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class LaunchJobsResult {

    private int status;
    private String errorMessage;
    private JobStatus launchStatus;
    private String launchMessage;
    private List<LaunchJob> jobs;

    @JsonProperty("Status")
    public int getStatus(){
        return this.status;
    }

    public void setStatus(int status){
        this.status = status;
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage(){
        return this.errorMessage;
    }

    public void setErrorMessage(String errorMessage){
        this.errorMessage = errorMessage;
    }

    @JsonProperty("LaunchStatus")
    public JobStatus getLaunchStatus(){
        return this.launchStatus;
    }

    @JsonProperty("LaunchStatus")
    public void setLaunchStatus(JobStatus launchStatus){
        this.launchStatus = launchStatus;
    }

    @JsonProperty("LaunchMessage")
    public String getLaunchMessage(){
        return this.launchMessage;
    }

    @JsonProperty("LaunchMessage")
    public void setLaunchMessage(String launchMessage){
        this.launchMessage = launchMessage;
    }

    @JsonProperty("Jobs")
    public List<LaunchJob> getJobs() {
        return jobs;
    }

    @JsonProperty("Jobs")
    public void setJobs(List<LaunchJob> jobs) {
        this.jobs = jobs;
    }

    public static class LaunchJob {

        private String tableName;
        private JobStatus status;
        private long extractedRows;
        private long totalRows;

        @JsonProperty("TableName")
        public String getTableName() {
            return tableName;
        }

        @JsonProperty("TableName")
        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        @JsonProperty("Status")
        public JobStatus getStatus() {
            return status;
        }

        @JsonProperty("Status")
        public void setStatus(JobStatus status) {
            this.status = status;
        }

        @JsonProperty("ExtractedRows")
        public long getExtractedRows() {
            return extractedRows;
        }

        @JsonProperty("ExtractedRows")
        public void setExtractedRows(long extractedRows) {
            this.extractedRows = extractedRows;
        }

        @JsonProperty("TotalRows")
        public long getTotalRows() {
            return totalRows;
        }

        @JsonProperty("TotalRows")
        public void setTotalRows(long totalRows) {
            this.totalRows = totalRows;
        }
    }
}
