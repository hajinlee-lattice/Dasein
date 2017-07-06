package com.latticeengines.domain.exposed.datacloud;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public class DataCloudJobConfiguration extends BasePayloadConfiguration {

    @JsonProperty("hdfs_pod_id")
    private String hdfsPodId;
    @JsonProperty("block_size")
    private Integer blockSize;
    @JsonProperty("group_size")
    private Integer groupSize;
    @JsonProperty("thread_pool_size")
    private Integer threadPoolSize;
    @JsonProperty("avro_path")
    private String avroPath;

    @JsonProperty("match_input")
    private MatchInput matchInput;

    @JsonProperty("root_operation_uid")
    private String rootOperationUid;
    @JsonProperty("block_operation_uid")
    private String blockOperationUid;
    @JsonProperty("app_name")
    private String appName;
    @JsonProperty("yarn_queue")
    private String yarnQueue;
    @JsonProperty("realtime_proxy_url")
    private String realTimeProxyUrl;

    private Schema inputAvroSchema;


    public String getHdfsPodId() {
        return hdfsPodId;
    }

    public void setHdfsPodId(String hdfsPodId) {
        this.hdfsPodId = hdfsPodId;
    }

    public Integer getBlockSize() {
        return blockSize;
    }

    public void setBlockSize(Integer blockSize) {
        this.blockSize = blockSize;
    }

    public Integer getGroupSize() {
        return groupSize;
    }

    public void setGroupSize(Integer groupSize) {
        this.groupSize = groupSize;
    }

    public Integer getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(Integer threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public String getAvroPath() {
        return avroPath;
    }

    public void setAvroPath(String avroPath) {
        this.avroPath = avroPath;
    }

    public MatchInput getMatchInput() {
        return matchInput;
    }

    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
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

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    public String getBlockOperationUid() {
        return blockOperationUid;
    }

    public void setBlockOperationUid(String blockOperationUid) {
        this.blockOperationUid = blockOperationUid;
    }

    public String getAppName() {
        return appName;
    }

    public void setAppName(String appName) {
        this.appName = appName;
    }

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public String getRealTimeProxyUrl() {
        return realTimeProxyUrl;
    }

    public void setRealTimeProxyUrl(String realTimeProxyUrl) {
        this.realTimeProxyUrl = realTimeProxyUrl;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }


}
