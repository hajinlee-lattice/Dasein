package com.latticeengines.domain.exposed.datacloud.match.patch;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.PatchBook;

import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * Base response entity for DataCloud Patcher
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PatchResponse {

    private static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ssZ";

    /**
     * Stats about the patch result
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class PatchStats {
        @JsonProperty("Total")
        private Integer total;

        @JsonProperty("StatusCounts")
        private Map<PatchStatus, Integer> statusMap;

        public Integer getTotal() {
            return total;
        }

        public void setTotal(Integer total) {
            this.total = total;
        }

        public Map<PatchStatus, Integer> getStatusMap() {
            return statusMap;
        }

        public void setStatusMap(Map<PatchStatus, Integer> statusMap) {
            this.statusMap = statusMap;
        }
    }

    @JsonProperty("ValidationResponse")
    private PatchValidationResponse validationResponse;

    @JsonProperty("Mode")
    private PatchMode mode;

    @JsonProperty("PatchBookType")
    private PatchBook.Type patchBookType;

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

    @JsonProperty("StartAt")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_FORMAT)
    private Date startAt;

    @JsonProperty("EndAt")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = DATE_FORMAT)
    private Date endAt;

    @JsonProperty("Duration")
    private String duration;

    @JsonProperty("Stats")
    private PatchStats stats;

    @JsonProperty("NotPatchedLogs")
    private List<PatchLog> notPatchedLogs;

    @JsonProperty("LogFilePath")
    private String logFilePath;

    public PatchValidationResponse getValidationResponse() {
        return validationResponse;
    }

    public void setValidationResponse(PatchValidationResponse validationResponse) {
        this.validationResponse = validationResponse;
    }

    public PatchMode getMode() {
        return mode;
    }

    public void setMode(PatchMode mode) {
        this.mode = mode;
    }

    public PatchBook.Type getPatchBookType() {
        return patchBookType;
    }

    public void setPatchBookType(PatchBook.Type patchBookType) {
        this.patchBookType = patchBookType;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public Date getStartAt() {
        return startAt;
    }

    public void setStartAt(Date startAt) {
        this.startAt = startAt;
    }

    public Date getEndAt() {
        return endAt;
    }

    public void setEndAt(Date endAt) {
        this.endAt = endAt;
    }

    public String getDuration() {
        return duration;
    }

    public void setDuration(String duration) {
        this.duration = duration;
    }

    public PatchStats getStats() {
        return stats;
    }

    public void setStats(PatchStats stats) {
        this.stats = stats;
    }

    public List<PatchLog> getNotPatchedLogs() {
        return notPatchedLogs;
    }

    public void setNotPatchedLogs(List<PatchLog> notPatchedLogs) {
        this.notPatchedLogs = notPatchedLogs;
    }

    public String getLogFilePath() {
        return logFilePath;
    }

    public void setLogFilePath(String logFilePath) {
        this.logFilePath = logFilePath;
    }
}
