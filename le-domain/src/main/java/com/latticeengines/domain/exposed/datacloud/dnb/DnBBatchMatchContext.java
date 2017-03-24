package com.latticeengines.domain.exposed.datacloud.dnb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DnBBatchMatchContext {

    private String serviceBatchId;

    private Date timestamp;

    private DnBReturnCode dnbCode;

    private Map<String, DnBMatchContext> contexts;

    private Long duration;

    private boolean logDnBBulkResult;

    private int retryTimes = 0;

    public DnBBatchMatchContext() {
        contexts = new HashMap<String, DnBMatchContext>();
    }

    public void copyForRetry(DnBBatchMatchContext batchContext) {
        logDnBBulkResult = batchContext.getLogDnBBulkResult();
        retryTimes = 1;
        for (String lookupRequestId : batchContext.getContexts().keySet()) {
            DnBMatchContext context = new DnBMatchContext();
            context.copyMatchInput(batchContext.getContexts().get(lookupRequestId));
            contexts.put(lookupRequestId, context);
        }
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

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }

    public boolean getLogDnBBulkResult() {
        return logDnBBulkResult;
    }

    public void setLogDnBBulkResult(boolean logDnBBulkResult) {
        this.logDnBBulkResult = logDnBBulkResult;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void setRetryTimes(int retryTimes) {
        this.retryTimes = retryTimes;
    }

}
