package com.latticeengines.domain.exposed.serviceflows.cdl.steps.integration;

import java.util.Date;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.integration.InboundConnectionType;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class BrokerDataFullLoadConfiguration extends BaseStepConfiguration {

    private CustomerSpace customerSpace;

    private Date startTime;

    private Date endTime;

    private InboundConnectionType inboundConnectionType;

    private String sourceId;

    private String bucket;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
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

    public InboundConnectionType getInboundConnectionType() {
        return inboundConnectionType;
    }

    public void setInboundConnectionType(InboundConnectionType inboundConnectionType) {
        this.inboundConnectionType = inboundConnectionType;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getBucket() {
        return bucket;
    }

    public void setBucket(String bucket) {
        this.bucket = bucket;
    }
}
