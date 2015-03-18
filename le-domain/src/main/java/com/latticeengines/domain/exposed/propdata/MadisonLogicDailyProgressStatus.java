package com.latticeengines.domain.exposed.propdata;


public enum MadisonLogicDailyProgressStatus {

    DEPIVOTED("Depivoted"), //
    DEPIVOTING("Depivoting"), //
    FINISHED("Finished"), //
    FAILED("Failed");
    
    private String status;

    private MadisonLogicDailyProgressStatus(String status) {
        this.status = status;
    }

    public String getStatus() {
        return status;
    }

}
