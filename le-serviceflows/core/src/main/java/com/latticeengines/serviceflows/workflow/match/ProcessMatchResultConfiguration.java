package com.latticeengines.serviceflows.workflow.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.serviceflows.workflow.dataflow.DataFlowStepConfiguration;

public class ProcessMatchResultConfiguration extends DataFlowStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;

    @JsonProperty("skip_dedupe")
    private boolean skipDedupe;

    public ProcessMatchResultConfiguration() {
        setBeanName("parseMatchResult");
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public boolean isSkipDedupe() {
        return skipDedupe;
    }

    public void setSkipDedupe(boolean skipDedupe) {
        this.skipDedupe = skipDedupe;
    }
}
