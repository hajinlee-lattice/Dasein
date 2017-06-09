package com.latticeengines.cdl.workflow.steps.importdata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ImportDataFeedTaskConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_task_id")
    private Long dataFeedTaskId;

//    @JsonProperty("staging_dir")
//    private String stagingDir;

    @JsonProperty("import_config")
    private String importConfig;

    public Long getDataFeedTaskId() {
        return dataFeedTaskId;
    }

    public void setDataFeedTaskId(Long dataFeedTaskId) {
        this.dataFeedTaskId = dataFeedTaskId;
    }

//    public String getStagingDir() {
//        return stagingDir;
//    }
//
//    public void setStagingDir(String stagingDir) {
//        this.stagingDir = stagingDir;
//    }

    public String getImportConfig() {
        return importConfig;
    }

    public void setImportConfig(String importConfig) {
        this.importConfig = importConfig;
    }
}
