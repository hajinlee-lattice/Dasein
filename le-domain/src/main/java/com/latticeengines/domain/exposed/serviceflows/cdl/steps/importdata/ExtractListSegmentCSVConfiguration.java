package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class ExtractListSegmentCSVConfiguration extends BaseStepConfiguration {

    public static String Direct_Phone = "Direct_Phone";

    private CustomerSpace customerSpace;

    private String segmentName;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getSegmentName() {
        return segmentName;
    }

    public void setSegmentName(String segmentName) {
        this.segmentName = segmentName;
    }

}
