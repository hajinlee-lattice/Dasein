package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CopyListSegmentCSVConfiguration extends BaseStepConfiguration {

    private CustomerSpace customerSpace;

    private String segmentName;

    private String sourceBucket;

    private String sourceKey;

    private String destBucket;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getSourceBucket() {
        return sourceBucket;
    }

    public void setSourceBucket(String sourceBucket) {
        this.sourceBucket = sourceBucket;
    }

    public String getSourceKey() {
        return sourceKey;
    }

    public void setSourceKey(String sourceKey) {
        this.sourceKey = sourceKey;
    }

    public String getDestBucket() {
        return destBucket;
    }

    public void setDestBucket(String destBucket) {
        this.destBucket = destBucket;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }
}
