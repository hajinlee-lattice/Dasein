package com.latticeengines.domain.exposed.propdata.match;

import java.util.List;

public class BulkMatchOutput {
    private String requestId;
    private List<MatchOutput> outputList;

    public String getRequestId() {
        return requestId;
    }

    public void setRequestId(String requestId) {
        this.requestId = requestId;
    }

    public List<MatchOutput> getOutputList() {
        return outputList;
    }

    public void setOutputList(List<MatchOutput> outputList) {
        this.outputList = outputList;
    }

}
