package com.latticeengines.domain.exposed.datacloud;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang.StringUtils;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;

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
    @JsonProperty("predefined_selection")
    private Predefined predefinedSelection;
    @JsonProperty("customized_selection")
    private ColumnSelection customizedSelection;
    @JsonProperty("key_map")
    private Map<MatchKey, List<String>> keyMap;
    @JsonProperty("root_operation_uid")
    private String rootOperationUid;
    @JsonProperty("block_operation_uid")
    private String blockOperationUid;
    @JsonProperty("app_name")
    private String appName;
    @JsonProperty("yarn_queue")
    private String yarnQueue;
    @JsonProperty("decision_graph")
    private String decisionGraph;
    @JsonProperty("data_cloud_version")
    private String dataCloudVersion;
    @JsonProperty("realtime_proxy_url")
    private String realTimeProxyUrl;

    private Schema inputAvroSchema;

    @JsonProperty("exclude_unmatched_public_domain")
    private Boolean excludeUnmatchedPublicDomain = Boolean.FALSE;
    @JsonProperty("public_domain_as_normal_domain")
    private Boolean publicDomainAsNormalDomain = Boolean.FALSE;
    @JsonProperty("use_realtime_proxy")
    private Boolean useRealTimeProxy = Boolean.FALSE;
    @JsonProperty("use_dnb_cache")
    private boolean useDnBCache = true;
    @JsonProperty("use_remote_dnb")
    private Boolean useRemoteDnB;
    @JsonProperty("log_dnb_bulk_Result")
    private boolean logDnBBulkResult = false;


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

    public Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    public void setPredefinedSelection(Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }
    
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public ColumnSelection getCustomizedSelection() {
        return customizedSelection;
    }

    public void setCustomizedSelection(ColumnSelection customizedSelection) {
        this.customizedSelection = customizedSelection;
    }

    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
        this.keyMap = keyMap;
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

    public Boolean getExcludeUnmatchedPublicDomain() {
        return excludeUnmatchedPublicDomain;
    }

    public void setExcludeUnmatchedPublicDomain(Boolean excludeUnmatchedPublicDomain) {
        this.excludeUnmatchedPublicDomain = excludeUnmatchedPublicDomain;
    }

    public Boolean getPublicDomainAsNormalDomain() {
        return publicDomainAsNormalDomain;
    }

    public void setPublicDomainAsNormalDomain(Boolean publicDomainAsNormalDomain) {
        this.publicDomainAsNormalDomain = publicDomainAsNormalDomain;
    }

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public String getDecisionGraph() {
        return decisionGraph;
    }

    public void setDecisionGraph(String decisionGraph) {
        this.decisionGraph = decisionGraph;
    }

    public String getRealTimeProxyUrl() {
        return realTimeProxyUrl;
    }

    public void setRealTimeProxyUrl(String realTimeProxyUrl) {
        this.realTimeProxyUrl = realTimeProxyUrl;
    }

    public Boolean getUseRealTimeProxy() {
        return useRealTimeProxy;
    }

    public void setUseRealTimeProxy(Boolean useRealTimeProxy) {
        this.useRealTimeProxy = useRealTimeProxy;
    }

    public boolean getUseDnBCache() {
        return useDnBCache;
    }

    public void setUseDnBCache(boolean useDnBCache) {
        this.useDnBCache = useDnBCache;
    }

    public void setUseRemoteDnB(Boolean useRemoteDnB) {
        this.useRemoteDnB = useRemoteDnB;
    }
    
    public Boolean getUseRemoteDnB() {
        return useRemoteDnB;
    }

    public boolean getLogDnBBulkResult() {
        return logDnBBulkResult;
    }

    public void setLogDnBBulkResult(boolean logDnBBulkResult) {
        this.logDnBBulkResult = logDnBBulkResult;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }


}
