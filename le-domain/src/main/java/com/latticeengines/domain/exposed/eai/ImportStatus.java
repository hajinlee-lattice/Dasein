package com.latticeengines.domain.exposed.eai;

public enum ImportStatus {
    SUBMITTED("Submitted"),
    RUNNING("Running"),
    FAILED("Failed"),
    SUCCESS("Success");

    private String status;

    ImportStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public static ImportStatus getStatus(String status) {
        ImportStatus result = null;
        for (ImportStatus importStatus : ImportStatus.values()) {
            if (status.equalsIgnoreCase(importStatus.getStatus())) {
                result = importStatus;
                break;
            }
        }
        return result;
    }
}
