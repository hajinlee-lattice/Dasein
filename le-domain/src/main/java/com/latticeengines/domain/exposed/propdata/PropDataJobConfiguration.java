package com.latticeengines.domain.exposed.propdata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;
import org.apache.commons.lang.StringUtils;

public class PropDataJobConfiguration extends BasePayloadConfiguration {

    private String hdfsPodId;
    private Integer blockSize;
    private Integer groupSize;
    private Integer threadPoolSize;
    private String avroPath;
    private ColumnSelection.Predefined predefinedSelection;
    private String predefinedSelectionVersion;
    private ColumnSelection customizedSelection;
    private Map<MatchKey, List<String>> keyMap;
    private String rootOperationUid;
    private String blockOperationUid;
    private String appName;
    private Boolean returnUnmatched;
    private String yarnQueue;
    private Schema inputAvroSchema;

    @JsonProperty("hdfs_pod_id")
    public String getHdfsPodId() {
        return hdfsPodId;
    }

    @JsonProperty("hdfs_pod_id")
    public void setHdfsPodId(String hdfsPodId) {
        this.hdfsPodId = hdfsPodId;
    }

    @JsonProperty("block_size")
    public Integer getBlockSize() {
        return blockSize;
    }

    @JsonProperty("block_size")
    public void setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
    }

    @JsonProperty("group_size")
    public Integer getGroupSize() {
        return groupSize;
    }

    @JsonProperty("group_size")
    public void setGroupSize(Integer groupSize) {
        this.groupSize = groupSize;
    }

    @JsonProperty("thread_pool_size")
    public Integer getThreadPoolSize() {
        return threadPoolSize;
    }

    @JsonProperty("thread_pool_size")
    public void setThreadPoolSize(Integer threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    @JsonProperty("avro_path")
    public String getAvroPath() {
        return avroPath;
    }

    @JsonProperty("avro_path")
    public void setAvroPath(String avroPath) {
        this.avroPath = avroPath;
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
            throw new RuntimeException("Failed to parse schema to json node.");
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

    @JsonProperty("block_operation_uid")
    public String getBlockOperationUid() {
        return blockOperationUid;
    }

    @JsonProperty("block_operation_uid")
    public void setBlockOperationUid(String blockOperationUid) {
        this.blockOperationUid = blockOperationUid;
    }

    @JsonProperty("app_name")
    public String getAppName() {
        return appName;
    }

    @JsonProperty("app_name")
    public void setAppName(String appName) {
        this.appName = appName;
    }

    @JsonProperty("return_unmatched")
    public Boolean getReturnUnmatched() {
        return returnUnmatched;
    }

    @JsonProperty("return_unmatched")
    public void setReturnUnmatched(Boolean returnUnmatched) {
        this.returnUnmatched = returnUnmatched;
    }

    @JsonProperty("yarn_queue")
    public String getYarnQueue() {
        return yarnQueue;
    }

    @JsonProperty("yarn_queue")
    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
