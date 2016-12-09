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
import com.latticeengines.domain.exposed.datacloud.match.MatchKey;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
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

    @NotNull
    private Boolean excludeUnmatchedPublicDomain;

    private Boolean publicDomainAsNormalDomain;

    private Predefined predefinedSelection;

    private String predefinedSelectionVersion;

    private ColumnSelection customizedSelection;

    private String yarnQueue;

    private String dataCloudVersion;
    private String decisionGraph;

    private Boolean useRealTimeProxy;
    private String realTimeProxyUrl;
    private Integer realTimeThreadPoolSize;

    private boolean useDnBCache = true;

    private boolean fuzzyMatchEnabled;

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
    public Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("predefined_selection")
    public void setPredefinedSelection(Predefined predefinedSelection) {
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

    @JsonProperty("data_cloud_version")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonProperty("data_cloud_version")
    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
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

    @JsonProperty("exclude_unmathced_public_domain")
    public Boolean getExcludeUnmatchedPublicDomain() {
        return excludeUnmatchedPublicDomain;
    }

    @JsonProperty("exclude_unmathced_public_domain")
    public void setExcludeUnmatchedPublicDomain(Boolean excludeUnmatchedPublicDomain) {
        this.excludeUnmatchedPublicDomain = excludeUnmatchedPublicDomain;
    }

    @JsonProperty("public_domain_as_normal_domain")
    public Boolean getPublicDomainAsNormalDomain() {
        return Boolean.TRUE.equals(publicDomainAsNormalDomain);
    }

    @JsonProperty("public_domain_as_normal_domain")
    public void setPublicDomainAsNormalDomain(Boolean publicDomainAsNormalDomain) {
        this.publicDomainAsNormalDomain = publicDomainAsNormalDomain;
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

    @JsonProperty("decision_graph")
    public String getDecisionGraph() {
        return decisionGraph;
    }

    @JsonProperty("decision_graph")
    public void setDecisionGraph(String decisionGraph) {
        this.decisionGraph = decisionGraph;
    }

    @JsonProperty("use_real_time_proxy")
    public Boolean getUseRealTimeProxy() {
        return useRealTimeProxy;
    }

    @JsonProperty("use_real_time_proxy")
    public void setUseRealTimeProxy(Boolean useRealTimeProxy) {
        this.useRealTimeProxy = useRealTimeProxy;
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

    @JsonProperty("use_dnb_cache")
    public boolean getUseDnBCache() {
        return useDnBCache;
    }

    @JsonProperty("use_dnb_cache")
    public void setUseDnBCache(boolean useDnBCache) {
        this.useDnBCache = useDnBCache;
    }

    @JsonProperty("fuzzy_match_enabled")
    public boolean isFuzzyMatchEnabled() {
        return fuzzyMatchEnabled;
    }

    @JsonProperty("fuzzy_match_enabled")
    public void setFuzzyMatchEnabled(boolean fuzzyMatchEnabled) {
        this.fuzzyMatchEnabled = fuzzyMatchEnabled;
    }

    
}

