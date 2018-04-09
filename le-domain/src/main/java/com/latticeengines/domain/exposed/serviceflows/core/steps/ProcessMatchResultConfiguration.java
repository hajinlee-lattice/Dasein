package com.latticeengines.domain.exposed.serviceflows.core.steps;

import com.fasterxml.jackson.annotation.JsonProperty;

public class ProcessMatchResultConfiguration extends BaseCoreDataFlowStepConfiguration {

    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;

    @JsonProperty("exclude_dc_attrs")
    private boolean excludeDataCloudAttrs;

    @JsonProperty("keep_lid")
    private boolean keepLid;

    public ProcessMatchResultConfiguration() {
        setBeanName("parseMatchResult");
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public boolean isExcludeDataCloudAttrs() {
        return excludeDataCloudAttrs;
    }

    public void setExcludeDataCloudAttrs(boolean excludeDataCloudAttrs) {
        this.excludeDataCloudAttrs = excludeDataCloudAttrs;
    }

    public boolean isKeepLid() {
        return keepLid;
    }

    public void setKeepLid(boolean keepLid) {
        this.keepLid = keepLid;
    }

}
