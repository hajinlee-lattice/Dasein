package com.latticeengines.domain.exposed.cdl.integration;


import java.util.Date;

public class BrokerFullLoadRequest {

    private String sourceId;

    private InboundConnectionType inboundConnectionType;

    private Date startTime;

    private Date endTime;

    private String bucket;

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public InboundConnectionType getInboundConnectionType() {
        return inboundConnectionType;
    }

    public void setInboundConnectionType(InboundConnectionType inboundConnectionType) {
        this.inboundConnectionType = inboundConnectionType;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
}
