package com.latticeengines.workflowapi.steps.prospectdiscovery;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.workflow.exposed.build.BaseStepConfiguration;

public class BaseFitModelStepConfiguration extends BaseStepConfiguration {

    private String customerSpace;
    private String microServiceHostPort;
    private String modelingServiceHdfsBaseDir;

    @JsonProperty("customerSpace")
    public String getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customerSpace")
    public void setCustomerSpace(String customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("microServiceHostPort")
    public String getMicroServiceHostPort() {
        return microServiceHostPort;
    }

    @JsonProperty("microServiceHostPort")
    public void setMicroServiceHostPort(String microServiceHostPort) {
        this.microServiceHostPort = microServiceHostPort;
    }

    @JsonProperty("modelingServiceHdfsBaseDir")
    public String getModelingServiceHdfsBaseDir() {
        return modelingServiceHdfsBaseDir;
    }

    @JsonProperty("modelingServiceHdfsBaseDir")
    public void setModelingServiceHdfsBaseDir(String modelingServiceHdfsBaseDir) {
        this.modelingServiceHdfsBaseDir = modelingServiceHdfsBaseDir;
    }
}
