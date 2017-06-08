package com.latticeengines.domain.exposed.serviceflows.datacloud.match.steps;

import java.io.IOException;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class PrepareBulkMatchInputConfiguration extends BaseStepConfiguration {

    @NotNull
    private MatchInput matchInput;

    @NotEmptyString
    @NotNull
    private String inputAvroDir;

    private Schema inputAvroSchema;

    @NotNull
    private CustomerSpace customerSpace;

    @NotEmptyString
    @NotNull
    private String rootOperationUid;

    @NotNull
    private Integer averageBlockSize;

    @NotEmptyString
    @NotNull
    private String hdfsPodId;

    private String yarnQueue;

    private String realTimeProxyUrl;
    private Integer realTimeThreadPoolSize;

    @JsonProperty("match_input")
    public MatchInput getMatchInput() {
        return matchInput;
    }

    @JsonProperty("match_input")
    public void setMatchInput(MatchInput matchInput) {
        this.matchInput = matchInput;
    }

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

    @JsonProperty("customer_space")
    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    @JsonProperty("customer_space")
    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
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

    @JsonProperty("real_time_proxy_url")
    public String getRealTimeProxyUrl() {
        return realTimeProxyUrl;
    }

    @JsonProperty("real_time_proxy_url")
    public void setRealTimeProxyUrl(String realTimeProxyUrl) {
        this.realTimeProxyUrl = realTimeProxyUrl;
    }

    @JsonProperty("real_time_thread_pool_size")
    public Integer getRealTimeThreadPoolSize() {
        return realTimeThreadPoolSize;
    }

    @JsonProperty("real_time_thread_pool_size")
    public void setRealTimeThreadPoolSize(Integer realTimeThreadPoolSize) {
        this.realTimeThreadPoolSize = realTimeThreadPoolSize;
    }
    
}

