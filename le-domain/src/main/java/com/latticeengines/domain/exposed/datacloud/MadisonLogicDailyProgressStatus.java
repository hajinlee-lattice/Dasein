package com.latticeengines.domain.exposed.datacloud;

public enum MadisonLogicDailyProgressStatus {

    DEPIVOTED("Depivoted"), //
    DEPIVOTING("Depivoting"), //
    FINISHED("Finished"), //
    FAILED("Failed");

    private String status;

    MadisonLogicDailyProgressStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

}
