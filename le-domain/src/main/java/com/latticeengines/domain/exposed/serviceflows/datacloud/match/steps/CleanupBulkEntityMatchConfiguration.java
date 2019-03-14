package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CleanupBulkEntityMatchConfiguration extends BaseStepConfiguration {

    @JsonProperty("TmpDir")
    private String tmpDir;

    @JsonProperty("PodId")
    private String podId;

    @JsonProperty("RootOperationUID")
    private String rootOperationUid;

    public String getTmpDir() {
        return tmpDir;
    }

    public void setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
    }

    public String getPodId() {
        return podId;
    }

    public void setPodId(String podId) {
        this.podId = podId;
    }

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }
}
