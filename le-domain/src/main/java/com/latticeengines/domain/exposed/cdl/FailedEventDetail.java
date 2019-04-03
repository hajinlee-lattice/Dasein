package com.latticeengines.domain.exposed.cdl;

import java.util.Map;

public class FailedEventDetail extends EventDetail {

    public FailedEventDetail() {
    }

    private Map<String, String> errorFile;

    public Map<String, String> getErrorFile() {
        return errorFile;
    }

    public void setErrorFile(Map<String, String> errorFile) {
        this.errorFile = errorFile;
    }
}
