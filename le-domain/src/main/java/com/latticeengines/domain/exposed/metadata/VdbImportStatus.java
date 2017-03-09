package com.latticeengines.domain.exposed.metadata;

public enum VdbImportStatus {
    SUBMITTED("Submitted"),
    RUNNING("Running"),
    FAILED("Failed"),
    SUCCESS("Success");

    private String status;

    VdbImportStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

    public static VdbImportStatus getStatus(String status) {
        VdbImportStatus result = null;
        for (VdbImportStatus importStatus : VdbImportStatus.values()) {
            if (status.equalsIgnoreCase(importStatus.getStatus())) {
                result = importStatus;
                break;
            }
        }
        return result;
    }
}
