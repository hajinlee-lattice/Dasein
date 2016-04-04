package com.latticeengines.propdata.workflow.steps;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PrepareBulkMatchInputConfiguration extends BaseStepConfiguration {

    @NotEmptyString
    @NotNull
    private String inputAvroDir;

    @NotNull
    private CustomerSpace customerSpace;

    @NotNull
    private Map<MatchKey, List<String>> keyMap;

    @NotEmptyString
    @NotNull
    private String rootOperationUid;

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    @NotNull
    private Integer groupSize;

    @NotNull
    private ColumnSelection.Predefined predefinedSelection;

    @NotEmptyString
    @NotNull
    private String targetDir;

    @JsonProperty("input_avro_dir")
    public String getInputAvroDir() {
        return inputAvroDir;
    }

    @JsonProperty("input_avro_dir")
    public void setInputAvroDir(String inputAvroDir) {
        this.inputAvroDir = inputAvroDir;
    }

    @JsonProperty("customer_space")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customer_space")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    @JsonProperty("key_map")
    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    @JsonProperty("key_map")
    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
        this.keyMap = keyMap;
    }

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

    @JsonProperty("group_size")
    public Integer getGroupSize() {
        return groupSize;
    }

    @JsonProperty("group_size")
    public void setGroupSize(Integer groupSize) {
        this.groupSize = groupSize;
    }

    @JsonProperty("predefined_selection")
    public ColumnSelection.Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("predefined_selection")
    public void setPredefinedSelection(ColumnSelection.Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }

    @JsonProperty("target_dir")
    public String getTargetDir() {
        return targetDir;
    }

    @JsonProperty("target_dir")
    public void setTargetDir(String targetDir) {
        this.targetDir = targetDir;
    }
}

