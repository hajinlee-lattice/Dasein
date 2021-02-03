package com.latticeengines.domain.exposed.serviceflows.cdl.steps.importdata;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class ExtractListSegmentCSVConfiguration extends BaseStepConfiguration {

    public static String Direct_Phone = "Direct_Phone";

    private CustomerSpace customerSpace;

    private String segmentName;

    private boolean isSSVITenant;

    private boolean isCDLTenant;

    private Map<String, List<String>> systemIdMaps;

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

    public boolean isSSVITenant() {
        return isSSVITenant;
    }

    public void setSSVITenant(boolean SSVITenant) {
        isSSVITenant = SSVITenant;
    }

    public boolean isCDLTenant() {
        return isCDLTenant;
    }

    public void setCDLTenant(boolean CDLTenant) {
        isCDLTenant = CDLTenant;
    }

    public Map<String, List<String>> getSystemIdMaps() {
        return systemIdMaps;
    }

    public void setSystemIdMaps(Map<String, List<String>> systemIdMaps) {
        this.systemIdMaps = systemIdMaps;
    }
}
