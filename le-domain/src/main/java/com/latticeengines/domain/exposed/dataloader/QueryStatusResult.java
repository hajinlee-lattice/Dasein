package com.latticeengines.domain.exposed.dataloader;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryStatusResult {

    private boolean success;
    private String errorMessage;
    private int status;
    private int progress;
    private String progressInfo;

    @JsonProperty("Success")
    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    @JsonProperty("ErrorMessage")
    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @JsonProperty("Status")
    public int getStatus(){
        return this.status;
    }

    public void setStatus(int status){
        this.status = status;
    }

    @JsonProperty("Progress")
    public int getProgress(){
        return this.progress;
    }

    public void setProgress(int progress){
        this.progress = progress;
    }

    @JsonProperty("ProgressInfo")
    public String getProgressInfo() {
        return progressInfo;
    }

    public void setProgressInfo(String progressInfo) {
        this.progressInfo = progressInfo;
    }
}
