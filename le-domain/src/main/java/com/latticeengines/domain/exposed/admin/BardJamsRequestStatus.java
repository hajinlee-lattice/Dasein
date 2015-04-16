package com.latticeengines.domain.exposed.admin;

public enum BardJamsRequestStatus {

    NEW("New"), //
    FINISHED("Finished"), //
    FAILED("Failed");

    private String status;

    private BardJamsRequestStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

}
