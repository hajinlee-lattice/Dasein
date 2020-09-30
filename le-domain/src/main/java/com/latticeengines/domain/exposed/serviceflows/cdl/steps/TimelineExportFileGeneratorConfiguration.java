package com.latticeengines.domain.exposed.serviceflows.cdl.steps;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class TimelineExportFileGeneratorConfiguration extends BaseStepConfiguration {

    @JsonProperty("customerspace")
    private CustomerSpace customerSpace;
    @JsonProperty("fieldnames")
    private List<String> fieldNames;
    @JsonProperty("namespace")
    private String nameSpace = "ExportTimeline";

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public List<String> getFieldNames() {
        return fieldNames;
    }

    public void setFieldNames(List<String> fieldNames) {
        this.fieldNames = fieldNames;
    }

    public String getNameSpace() {
        return nameSpace;
    }

    public void setNameSpace(String nameSpace) {
        this.nameSpace = nameSpace;
    }
}
