package com.latticeengines.domain.exposed.datacloud.match;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricFieldGroup;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class MatchInput implements Fact, Dimension {

    public static final String DEFAULT_DATACLOUD_VERSION = "1.0.0";

    private Tenant tenant;

    private List<String> fields;
    private List<List<Object>> data;
    private int numRows;

    private Boolean excludeUnmatchedWithPublicDomain;
    private Boolean publicDomainAsNormalDomain;
    private Boolean fetchOnly;
    private Boolean skipKeyResolution;
    private String decisionGraph;
    private Level logLevel;

    private String rootOperationUid;
    private String tableName;
    private boolean bulkOnly;
    private String matchEngine;

    // optional, but better to provide. if not, will be resolved from the fields
    private Map<MatchKey, List<String>> keyMap;

    // only one of these is needed, custom selection has higher priority
    private Predefined predefinedSelection;
    private ColumnSelection customSelection;
    private UnionSelection unionSelection;

    // if not provided, pick latest
    private String predefinedVersion;
    private String dataCloudVersion = DEFAULT_DATACLOUD_VERSION;
    private Integer numSelectedColumns;

    // only applicable for bulk match
    private InputBuffer inputBuffer;
    private IOBufferType outputBufferType;
    private String yarnQueue;

    // testing via real time proxy
    private Boolean useRealTimeProxy;
    private String realTimeProxyUrl;
    private Integer realTimeThreadPoolSize;

    // if not provided, first check DnB cache before going to DnB api
    private boolean useDnBCache = true;
    // Flag useRemoteDnB decides whether go to DnB api.
    // Purpose of this flag: If feature of using fuzzy match is turned off, DnB
    // cache is used to do exact location lookup, but DnB api is not called
    private Boolean useRemoteDnB;
    // Flag logDnBBulkResult decides whether DnB bulk match result is logged
    private boolean logDnBBulkResult = false;

    // Flag to add DnB columns match output file
    private boolean matchDebugEnabled;

    @JsonProperty("ExcludeUnmatchedWithPublicDomain")
    public Boolean getExcludeUnmatchedWithPublicDomain() {
        return Boolean.TRUE.equals(excludeUnmatchedWithPublicDomain);
    }

    @JsonProperty("ExcludeUnmatchedWithPublicDomain")
    public void setExcludeUnmatchedWithPublicDomain(Boolean excludeUnmatchedWithPublicDomain) {
        this.excludeUnmatchedWithPublicDomain = Boolean.TRUE.equals(excludeUnmatchedWithPublicDomain);
    }

    @JsonProperty("FetchOnly")
    public Boolean getFetchOnly() {
        return Boolean.TRUE.equals(fetchOnly);
    }

    @JsonProperty("FetchOnly")
    public void setFetchOnly(Boolean fetchOnly) {
        this.fetchOnly = fetchOnly;
    }

    @JsonProperty("PublicDomainAsNormalDomain")
    public Boolean getPublicDomainAsNormalDomain() {
        return Boolean.TRUE.equals(publicDomainAsNormalDomain);
    }

    @JsonProperty("PublicDomainAsNormalDomain")
    public void setPublicDomainAsNormalDomain(Boolean publicDomainAsNormalDomain) {
        this.publicDomainAsNormalDomain = publicDomainAsNormalDomain;
    }

    @JsonProperty("SkipKeyResolution")
    public Boolean getSkipKeyResolution() {
        return skipKeyResolution;
    }

    @JsonProperty("SkipKeyResolution")
    public void setSkipKeyResolution(Boolean skipKeyResolution) {
        this.skipKeyResolution = skipKeyResolution;
    }

    @JsonProperty("DecisionGraph")
    public String getDecisionGraph() {
        return decisionGraph;
    }

    @JsonProperty("DecisionGraph")
    public void setDecisionGraph(String decisionGraph) {
        this.decisionGraph = decisionGraph;
    }

    @JsonIgnore
    public Level getLogLevel() {
        return logLevel;
    }

    @JsonIgnore
    public void setLogLevel(Level logLevel) {
        this.logLevel = logLevel;
    }

    @JsonProperty("LogLevel")
    private String getLogLevelAsString() {
        return logLevel != null ? logLevel.toString() : null;
    }

    @JsonProperty("LogLevel")
    private void setLogLevelByString(String logLevel) {
        if (StringUtils.isNotEmpty(logLevel)) {
            this.logLevel = Level.toLevel(logLevel);
        } else {
            this.logLevel = null;
        }
    }

    @JsonProperty("KeyMap")
    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    @JsonProperty("KeyMap")
    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
        this.keyMap = keyMap;
    }

    @JsonProperty("Fields")
    public List<String> getFields() {
        return fields;
    }

    @JsonProperty("Fields")
    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @JsonIgnore
    @MetricField(name = "InputFields", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumInputFields() {
        return getFields().size();
    }

    @JsonProperty("Data")
    public List<List<Object>> getData() {
        return data;
    }

    @JsonProperty("Data")
    public void setData(List<List<Object>> data) {
        this.data = data;
        setNumRows(data.size());
    }

    @JsonProperty("InputBuffer")
    public InputBuffer getInputBuffer() {
        return inputBuffer;
    }

    @JsonProperty("InputBuffer")
    public void setInputBuffer(InputBuffer buffer) {
        this.inputBuffer = buffer;
    }

    @JsonProperty("OutputBufferType")
    public IOBufferType getOutputBufferType() {
        return outputBufferType;
    }

    @JsonProperty("OutputBufferType")
    public void setOutputBufferType(IOBufferType outputBufferType) {
        this.outputBufferType = outputBufferType;
    }

    @JsonIgnore
    @MetricField(name = "InputRows", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumRows() {
        return numRows;
    }

    @JsonIgnore
    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    @MetricFieldGroup
    @JsonProperty("Tenant")
    public Tenant getTenant() {
        return tenant;
    }

    @JsonProperty("Tenant")
    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @MetricTagGroup
    @JsonProperty("PredefinedSelection")
    public Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    @JsonProperty("PredefinedSelection")
    public void setPredefinedSelection(Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }

    @JsonProperty("PredefinedVersion")
    public String getPredefinedVersion() {
        return predefinedVersion;
    }

    @JsonProperty("PredefinedVersion")
    public void setPredefinedVersion(String predefinedVersion) {
        this.predefinedVersion = predefinedVersion;
    }

    @JsonProperty("DataCloudVersion")
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    @JsonProperty("DataCloudVersion")
    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    @JsonProperty("CustomSelection")
    public ColumnSelection getCustomSelection() {
        return customSelection;
    }

    @JsonProperty("CustomSelection")
    public void setCustomSelection(ColumnSelection customSelection) {
        this.customSelection = customSelection;
    }

    @JsonProperty("UnionSelections")
    public UnionSelection getUnionSelection() {
        return unionSelection;
    }

    @JsonProperty("UnionSelections")
    public void setUnionSelection(UnionSelection unionSelection) {
        this.unionSelection = unionSelection;
    }

    @MetricTag(tag = "MatchEngine")
    @JsonIgnore
    public String getMatchEngine() {
        return matchEngine;
    }

    @JsonIgnore
    public void setMatchEngine(String matchEngine) {
        this.matchEngine = matchEngine;
    }

    @JsonIgnore
    @MetricField(name = "SelectedColumns", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumSelectedColumns() {
        return numSelectedColumns;
    }

    @JsonIgnore
    public void setNumSelectedColumns(Integer numSelectedColumns) {
        this.numSelectedColumns = numSelectedColumns;
    }

    @JsonProperty("RootOperationUID")
    public String getRootOperationUid() {
        return rootOperationUid;
    }

    @JsonProperty("RootOperationUID")
    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    @JsonProperty("YarnQueue")
    public String getYarnQueue() {
        return yarnQueue;
    }

    @JsonProperty("YarnQueue")
    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    @JsonProperty("TableName")
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    @JsonProperty("TableName")
    public String getTableName() {
        return tableName;
    }

    @JsonProperty("BulkOnly")
    public boolean isBulkOnly() {
        return bulkOnly;
    }

    @JsonProperty("BulkOnly")
    public void setBulkOnly(boolean bulkOnly) {
        this.bulkOnly = bulkOnly;
    }

    @JsonProperty("UseRealTimeProxy")
    public Boolean getUseRealTimeProxy() {
        return useRealTimeProxy;
    }

    @JsonProperty("UseRealTimeProxy")
    public void setUseRealTimeProxy(Boolean useRealTimeProxy) {
        this.useRealTimeProxy = useRealTimeProxy;
    }

    @JsonProperty("RealTimeProxyUrl")
    public String getRealTimeProxyUrl() {
        return realTimeProxyUrl;
    }

    @JsonProperty("RealTimeProxyUrl")
    public void setRealTimeProxyUrl(String realTimeProxyUrl) {
        this.realTimeProxyUrl = realTimeProxyUrl;
    }

    @JsonProperty("RealTimeThreadPoolSize")
    public Integer getRealTimeThreadPoolSize() {
        return realTimeThreadPoolSize;
    }

    @JsonProperty("RealTimeThreadPoolSize")
    public void setRealTimeThreadPoolSize(Integer realTimeThreadPoolSize) {
        this.realTimeThreadPoolSize = realTimeThreadPoolSize;
    }

    public static String getDefaultDatacloudVersion() {
        return DEFAULT_DATACLOUD_VERSION;
    }

    @JsonProperty("UseDnBCache")
    public boolean getUseDnBCache() {
        return useDnBCache;
    }

    @JsonProperty("UseDnBCache")
    public void setUseDnBCache(boolean useDnBCache) {
        this.useDnBCache = useDnBCache;
    }

    @JsonProperty("FuzzyMatchEnabled")
    public void setUseRemoteDnB(Boolean useRemoteDnB) {
        this.useRemoteDnB = useRemoteDnB;
    }

    @JsonProperty("FuzzyMatchEnabled")
    public Boolean getUseRemoteDnB() {
        return useRemoteDnB;
    }

    @JsonProperty("LogDnBBulkResult")
    public boolean getLogDnBBulkResult() {
        return logDnBBulkResult;
    }

    @JsonProperty("LogDnBBulkResult")
    public void setLogDnBBulkResult(boolean logDnBBulkResult) {
        this.logDnBBulkResult = logDnBBulkResult;
    }

    @JsonProperty("MatchDebugEnable")
    public boolean isMatchDebugEnabled() {
        return matchDebugEnabled;
    }

    @JsonProperty("MatchDebugEnable")
    public void setMatchDebugEnabled(boolean matchDebugEnabled) {
        this.matchDebugEnabled = matchDebugEnabled;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
