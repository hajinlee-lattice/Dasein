package com.latticeengines.serviceflows.workflow.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class ProcessMatchResultConfiguration extends DataFlowStepConfiguration {

    private String dataCloudVersion;

    public ProcessMatchResultConfiguration() {
        setBeanName("parseMatchResult");
    }

    @JsonProperty("data_cloud_version")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonProperty("data_cloud_version")
    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

}
