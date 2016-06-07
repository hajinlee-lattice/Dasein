package com.latticeengines.propdata.workflow.match.steps;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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

    private Schema inputAvroSchema;

    @NotNull
    private CustomerSpace customerSpace;

    @NotNull
    private Map<MatchKey, List<String>> keyMap;

    @NotEmptyString
    @NotNull
    private String rootOperationUid;

    @NotNull
    private Integer averageBlockSize;

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    @NotNull
    private Boolean returnUnmatched;

    private ColumnSelection.Predefined predefinedSelection;

    private String predefinedSelectionVersion;

    private ColumnSelection customizedSelection;

    private String yarnQueue;

    @JsonProperty("input_avro_dir")
    public String getInputAvroDir() {
        return inputAvroDir;
    }

    @JsonProperty("input_avro_dir")
    public void setInputAvroDir(String inputAvroDir) {
        this.inputAvroDir = inputAvroDir;
    }

    @JsonProperty("input_avro_schema")
    private JsonNode getInputAvroSchemaAsJson() {
        try {
            if (inputAvroSchema != null) {
                return new ObjectMapper().readTree(inputAvroSchema.toString());
            } else {
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException("Faild to parst schema to json node");
        }
    }

    @JsonProperty("input_avro_schema")
    private void setInputAvroSchemaAsJson(JsonNode inputAvroSchema) {
        if (inputAvroSchema != null && StringUtils.isNotEmpty(inputAvroSchema.toString())
                && !"null".equalsIgnoreCase(inputAvroSchema.toString())) {
            this.inputAvroSchema = new Schema.Parser().parse(inputAvroSchema.toString());
        } else  {
            this.inputAvroSchema = null;
        }
    }

    @JsonIgnore
    public Schema getInputAvroSchema() {
        return inputAvroSchema;
    }

    @JsonIgnore
    public void setInputAvroSchema(Schema inputAvroSchema) {
        this.inputAvroSchema = inputAvroSchema;
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

    @JsonProperty("predefined_selection")
    public ColumnSelection.Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("predefined_selection")
    public void setPredefinedSelection(ColumnSelection.Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }

    @JsonProperty("predefined_selection_version")
    public String getPredefinedSelectionVersion() {
        return predefinedSelectionVersion;
    }

    @JsonProperty("predefined_selection_version")
    public void setPredefinedSelectionVersion(String predefinedSelectionVersion) {
        this.predefinedSelectionVersion = predefinedSelectionVersion;
    }

    @JsonProperty("customized_selection")
    public ColumnSelection getCustomizedSelection() {
        return customizedSelection;
    }

    @JsonProperty("customized_selection")
    public void setCustomizedSelection(ColumnSelection customizedSelection) {
        this.customizedSelection = customizedSelection;
    }

    @JsonProperty("return_unmatched")
    public Boolean getReturnUnmatched() {
        return returnUnmatched;
    }

    @JsonProperty("return_unmatched")
    public void setReturnUnmatched(Boolean returnUnmatched) {
        this.returnUnmatched = returnUnmatched;
    }

    @JsonProperty("average_block_size")
    public Integer getAverageBlockSize() {
        return averageBlockSize;
    }

    @JsonProperty("average_block_size")
    public void setAverageBlockSize(Integer averageBlockSize) {
        this.averageBlockSize = averageBlockSize;
    }

    @JsonProperty("yarn_queue")
    public String getYarnQueue() {
        return yarnQueue;
    }

    @JsonProperty("yarn_queue")
    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }
}

