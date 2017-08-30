package com.latticeengines.domain.exposed.eai;

import com.fasterxml.jackson.annotation.JsonValue;

public enum ImportStatus {
    SUBMITTED("Submitted", false),
    RUNNING("Running", false),
    FAILED("Failed", true),
    SUCCESS("Success", true),
    WAITINGREGISTER("WaitingRegister", false);

    private String status;
    private boolean terminated;

    ImportStatus(String status, boolean terminated) {
        this.status = status;
        this.terminated = terminated;
    }

    public String getStatus() {
        return status;
    }

    public boolean isTerminated() {
        return terminated;
    }

    @JsonValue
    public String getName() {
        return super.name().toUpperCase();
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
