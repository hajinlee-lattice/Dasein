package com.latticeengines.domain.exposed.admin;

public enum BardJamsTenantStatus {

    NEW("New"), //
    FINISHED("Finished"), //
    FAILED("Failed");

    private String status;

    private BardJamsTenantStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

}
