package com.latticeengines.domain.exposed.cdl;

import java.util.Map;

public class FailedEventDetail extends EventDetail {

    public FailedEventDetail() {
        super("Failed");
    }

    private Map<String, String> errorFile;

    private Map<String, Object> error;

    public Map<String, String> getErrorFile() {
        return errorFile;
    }

    public void setErrorFile(Map<String, String> errorFile) {
        this.errorFile = errorFile;
    }

    public Map<String, Object> getError() {
        return error;
    }

    public void setError(Map<String, Object> error) {
        this.error = error;
    }
}
