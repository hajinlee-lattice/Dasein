package com.latticeengines.domain.exposed.propdata;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.match.MatchKey;

public class PropDataJobConfiguration extends BasePayloadConfiguration {

    private String hdfsPodId;
    private Integer blockSize;
    private Integer groupSize;
    private Integer threadPoolSize;
    private String avroPath;
    private ColumnSelection.Predefined predefinedSelection;
    private Map<MatchKey, List<String>> keyMap;
    private String rootOperationUid;
    private String blockOperationUid;
    private String appName;
    private Boolean singleBlock;
    private Boolean returnUnmatched;
    private String yarnQueue;

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

    @JsonProperty("predefined_selection")
    public ColumnSelection.Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("predefined_selection")
    public void setPredefinedSelection(ColumnSelection.Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
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

    @JsonProperty("single_block")
    public Boolean getSingleBlock() {
        return singleBlock;
    }

    @JsonProperty("single_block")
    public void setSingleBlock(Boolean singleBlock) {
        this.singleBlock = singleBlock;
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
