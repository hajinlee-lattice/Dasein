package com.latticeengines.domain.exposed.cdl;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProgressEventDetail extends EventDetail {

    public ProgressEventDetail() {
        super("Progress");
    }

    private Long batchId;

    private String importId;

    private String status;

    private Long processed;

    private Long failed;

    private Long warning;

    private Long duplicates;

    private String message;

    private Map<String, String> errorFile;

    @JsonProperty("total_records_submitted")
    private Long totalRecordsSubmitted;

    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public Map<String, String> getErrorFile() {
        return errorFile;
    }

    public void setErrorFile(Map<String, String> errorFile) {
        this.errorFile = errorFile;
    }

    public String getImportId() {
        return importId;
    }

    public void setImportId(String importId) {
        this.importId = importId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getProcessed() {
        return processed;
    }

    public void setProcessed(Long processed) {
        this.processed = processed;
    }

    public Long getFailed() {
        return failed;
    }

    public void setFailed(Long failed) {
        this.failed = failed;
    }

    public Long getWarning() {
        return warning;
    }

    public void setWarning(Long warning) {
        this.warning = warning;
    }

    public Long getDuplicates() {
        return duplicates;
    }

    public void setDuplicates(Long duplicates) {
        this.duplicates = duplicates;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public Long getTotalRecordsSubmitted() {
        return totalRecordsSubmitted;
    }

    public void setTotalRecordsSubmitted(Long totalRecordsSubmitted) {
        this.totalRecordsSubmitted = totalRecordsSubmitted;
    }
}
