package com.latticeengines.scoringharness.cloudmodel;

import java.util.ArrayList;

public class BaseCloudResult {
    public boolean isSuccess = false;
    public String requestId = null;
    public ArrayList<String> jsonObjectResults = null;
    public String errorMessage = null;

    public BaseCloudResult(boolean isSuccess, String requestId, ArrayList<String> jsonObjectResults) {
        this.isSuccess = isSuccess;
        this.requestId = requestId;
        this.jsonObjectResults = jsonObjectResults;
    }

    public BaseCloudResult(boolean isSuccess, String requestId, String errorMessage) {
        this.isSuccess = isSuccess;
        this.requestId = requestId;
        this.errorMessage = errorMessage;
    }
}
