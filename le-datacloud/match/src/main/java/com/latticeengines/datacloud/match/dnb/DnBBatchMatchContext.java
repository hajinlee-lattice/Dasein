package com.latticeengines.datacloud.match.dnb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DnBBatchMatchContext {

    private String serviceBatchId;

    private Date timestamp;

    private DnBReturnCode dnbCode;

    private Map<String, DnBMatchContext> contexts;

    public DnBBatchMatchContext() {
        contexts = new HashMap<String, DnBMatchContext>();
    }

    public String getServiceBatchId() {
        return serviceBatchId;
    }

    public void setServiceBatchId(String serviceBatchId) {
        this.serviceBatchId = serviceBatchId;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
    }

    public Map<String, DnBMatchContext> getContexts() {
        return contexts;
    }

    public void setContexts(Map<String, DnBMatchContext> contexts) {
        this.contexts = contexts;
    }

}
