package com.latticeengines.domain.exposed.dataplatform;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationResourceUsageReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class JobStatus implements HasId<String> {

    private String jobId;
    private String resultDirectory;
    private String dataDiagnosticsPath;
    private FinalApplicationStatus status;
    private YarnApplicationState state;
    private float progress;
    private String diagnostics;
    private String trackingUrl;
    private long startTime;
    private long finishTime;
    private ApplicationResourceUsageReport appResUsgReport;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String getId() {
        return jobId;
    }

    @Override
    public void setId(String jobId) {
        this.jobId = jobId;
    }

    @JsonProperty("status")
    public FinalApplicationStatus getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(FinalApplicationStatus status) {
        this.status = status;
    }

    @JsonProperty("state")
    public YarnApplicationState getState() {
        return state;
    }

    @JsonProperty("state")
    public void setState(YarnApplicationState state) {
        this.state = state;
    }

    @JsonProperty("result_dir")
    public String getResultDirectory() {
        return resultDirectory;
    }

    @JsonProperty("result_dir")
    public void setResultDirectory(String resultDirectory) {
        this.resultDirectory = resultDirectory;
    }

    @JsonProperty("diagnostics_path")
    public String getDataDiagnosticsPath() {
        return dataDiagnosticsPath;
    }

    @JsonProperty("diagnostics_path")
    public void setDataDiagnosticsPath(String dataDiagnosticsPath) {
        this.dataDiagnosticsPath = dataDiagnosticsPath;
    }

    @JsonProperty("progress")
    public float getProgress() {
        return progress;
    }

    @JsonProperty("progress")
    public void setProgress(float progress) {
        this.progress = progress;
    }

    @JsonProperty("diagnostics")
    public String getDiagnostics() {
        return diagnostics;
    }

    @JsonProperty("diagnostics")
    public void setDiagnostics(String diagnostics) {
        this.diagnostics = diagnostics;
    }

    @JsonProperty("tracking_url")
    public String getTrackingUrl() {
        return trackingUrl;
    }

    @JsonProperty("tracking_url")
    public void setTrackingUrl(String trackingUrl) {
        this.trackingUrl = trackingUrl;
    }

    @JsonProperty("start_time")
    public long getStartTime() {
        return startTime;
    }

    @JsonProperty("start_time")
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    @JsonProperty("end_time")
    public long getFinishTime() {
        return finishTime;
    }

    @JsonProperty("end_time")
    public void setFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    @JsonIgnore
    public ApplicationResourceUsageReport getAppResUsageReport() {
        return this.appResUsgReport;
    }

    @JsonIgnore
    public void setAppResUsageReport(ApplicationResourceUsageReport appResUsgReport) {
        this.appResUsgReport = appResUsgReport;
    }

    public String getErrorReport() {
        StringBuffer errorReport = new StringBuffer();
        errorReport.append(status.toString());
        if (StringUtils.isNotBlank(diagnostics)) {
            errorReport.append(": ");
            errorReport.append(diagnostics);
        }
        return errorReport.toString();
    }
}
