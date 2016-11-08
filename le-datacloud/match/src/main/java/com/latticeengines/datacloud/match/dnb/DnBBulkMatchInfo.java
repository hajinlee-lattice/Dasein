package com.latticeengines.datacloud.match.dnb;

import java.util.List;

public class DnBBulkMatchInfo {
    private String serviceBatchId;

    // Format: yyyy-MM-dd'T'HH:mm:ss.S'Z'
    private String timestamp;

    private DnBReturnCode dnbCode;

    private List<String> lookupRequestIds;

    public String getServiceBatchId() {
        return serviceBatchId;
    }

    public void setServiceBatchId(String serviceBatchId) {
        this.serviceBatchId = serviceBatchId;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
    }

    public List<String> getLookupRequestIds() {
        return lookupRequestIds;
    }

    public void setLookupRequestIds(List<String> lookupRequestIds) {
        this.lookupRequestIds = lookupRequestIds;
    }

}
