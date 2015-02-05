package com.latticeengines.scoringharness.cloudmodel;

import com.fasterxml.jackson.databind.node.ArrayNode;

public class BaseCloudResult {
    public boolean isSuccess = false;
    public String requestId = null;
    public ArrayNode results = null;
    public String errorMessage = null;

    public BaseCloudResult(boolean isSuccess, String requestId, ArrayNode results) {
        this.isSuccess = isSuccess;
        this.requestId = requestId;
        this.results = results;
    }

    public BaseCloudResult(boolean isSuccess, String requestId, String errorMessage) {
        this.isSuccess = isSuccess;
        this.requestId = requestId;
        this.errorMessage = errorMessage;
    }
}
