package com.latticeengines.domain.exposed.cdl;

public class FailedEventDetail extends EventDetail {

    public FailedEventDetail() {
    }

    private String errorFile;

    public String getErrorFile() {
        return errorFile;
    }

    public void setErrorFile(String errorFile) {
        this.errorFile = errorFile;
    }
}
