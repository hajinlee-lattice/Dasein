package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.serviceflows.core.steps.DataFlowStepConfiguration;

public class CascadingBulkMatchStepConfiguration extends DataFlowStepConfiguration {

    @NotEmptyString
    @NotNull
    private String rootOperationUid;

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    private String yarnQueue;

    private Map<String, String> inputProperties;

    @JsonProperty("root_operation_uid")
    public String getRootOperationUid() {
        return rootOperationUid;
    }

    @JsonProperty("root_operation_uid")
    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    @JsonProperty("hdfs_pod_id")
    public String getHdfsPodId() {
        return hdfsPodId;
    }

    @JsonProperty("hdfs_pod_id")
    public void setHdfsPodId(String hdfsPodId) {
        this.hdfsPodId = hdfsPodId;
    }

    @JsonProperty("yarn_queue")
    public String getYarnQueue() {
        return yarnQueue;
    }

    @JsonProperty("yarn_queue")
    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    @JsonProperty("input_properties")
    public void setInputProperties(Map<String, String> inputProperties) {
        this.inputProperties = inputProperties;
    }

    @JsonProperty("input_properties")
    public Map<String, String> getInputProperties() {
        return inputProperties;
    }

}
