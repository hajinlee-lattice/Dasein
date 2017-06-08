package com.latticeengines.serviceflows.workflow.match;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class ProcessMatchResultConfiguration extends DataFlowStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;

    @JsonProperty("skip_dedupe")
    private boolean skipDedupe;

    @JsonProperty("exclude_dc_attrs")
    private boolean excludeDataCloudAttrs;

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

    public boolean isExcludeDataCloudAttrs() {
        return excludeDataCloudAttrs;
    }

    public void setExcludeDataCloudAttrs(boolean excludeDataCloudAttrs) {
        this.excludeDataCloudAttrs = excludeDataCloudAttrs;
    }
}
