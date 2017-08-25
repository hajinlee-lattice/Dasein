package com.latticeengines.domain.exposed.datacloud.dnb;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class DnBBatchMatchContext {

    private String rootOperationUid;

    private String serviceBatchId;

    private Date timestamp;

    private Date createTime;

    private DnBReturnCode dnbCode;

    private Map<String, DnBMatchContext> contexts;

    private Long duration;

    private boolean logDnBBulkResult;

    private int retryTimes = 0;

    private String retryForServiceBatchId;

    private boolean sealed; // contexts is fixed, no longer add new context in

    public DnBBatchMatchContext() {
        contexts = new HashMap<>();
        createTime = new Date();
    }

    public void copyForRetry(DnBBatchMatchContext batchContext) {
        logDnBBulkResult = batchContext.getLogDnBBulkResult();
        retryTimes = 1;
        retryForServiceBatchId = batchContext.getServiceBatchId();
        sealed = true;
        createTime = new Date();
        for (String lookupRequestId : batchContext.getContexts().keySet()) {
            DnBMatchContext context = new DnBMatchContext();
            context.copyMatchInput(batchContext.getContexts().get(lookupRequestId));
            contexts.put(lookupRequestId, context);
        }
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
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

    public String getRetryForServiceBatchId() {
        return retryForServiceBatchId;
    }

    public void setRetryForServiceBatchId(String retryForServiceBatchId) {
        this.retryForServiceBatchId = retryForServiceBatchId;
    }

    public boolean isSealed() {
        return sealed;
    }

    public void setSealed(boolean sealed) {
        this.sealed = sealed;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

}
