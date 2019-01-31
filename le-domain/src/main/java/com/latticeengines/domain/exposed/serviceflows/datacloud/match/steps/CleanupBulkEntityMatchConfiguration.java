package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CleanupBulkEntityMatchConfiguration extends BaseStepConfiguration {

    @JsonProperty("TmpDir")
    private String tmpDir;

    public String getTmpDir() {
        return tmpDir;
    }

    public void setTmpDir(String tmpDir) {
        this.tmpDir = tmpDir;
    }
}
