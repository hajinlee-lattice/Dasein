package com.latticeengines.domain.exposed.admin;

public enum BardJamsTenantStatus {

    NEW("New"), //
    FINISHED("Succeeded"), //
    FAILED("Failed"), //
    UNINSTALLED("Uninstalled"); //

    private String status;

    BardJamsTenantStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

}
