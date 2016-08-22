package com.latticeengines.domain.exposed.propdata.match;

import java.util.List;

public class BulkMatchInput {
    private String requestId;
    private List<MatchInput> inputList;
    private boolean homogeneous;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<MatchInput> getInputList() {
        return inputList;
    }

    public void setInputList(List<MatchInput> inputList) {
        this.inputList = inputList;
    }

    public boolean isHomogeneous() {
        return homogeneous;
    }

    public void setHomogeneous(boolean homogeneous) {
        this.homogeneous = homogeneous;
    }
}
