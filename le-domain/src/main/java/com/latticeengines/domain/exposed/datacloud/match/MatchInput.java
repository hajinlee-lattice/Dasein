package com.latticeengines.domain.exposed.datacloud.match;

import java.util.Collections;
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

    @JsonProperty("Tenant")
    private Tenant tenant;

    @JsonProperty("Fields")
    private List<String> fields;

    @JsonProperty("Data")
    private List<List<Object>> data;

    @JsonIgnore
    private int numRows;

    @JsonProperty("DecisionGraph")
    private String decisionGraph;

    @JsonIgnore
    private Level logLevel;

    @JsonProperty("RootOperationUid")
    private String rootOperationUid;

    @JsonIgnore
    private String matchEngine;

    // optional, but better to provide. if not, will be resolved from the fields
    @JsonProperty("KeyMap")
    private Map<MatchKey, List<String>> keyMap;

    // only one of these is needed, custom selection has higher priority
    @JsonProperty("PredefinedSelection")
    private Predefined predefinedSelection;

    @JsonProperty("CustomSelection")
    private ColumnSelection customSelection;

    @JsonProperty("UnionSelection")
    private UnionSelection unionSelection;

    @JsonProperty("TimeOut")
    private Long timeout;

    @JsonProperty("RequestSource")
    private MatchRequestSource requestSource = MatchRequestSource.SCORING;

    // legacy configuration to be removed
    @JsonIgnore
    private static final String predefinedVersion = "1.0";

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion = DEFAULT_DATACLOUD_VERSION;

    @JsonIgnore
    private Integer numSelectedColumns;

    // only applicable for bulk match
    @JsonProperty("InputBuffer")
    private InputBuffer inputBuffer;

    @JsonProperty("OutputBufferType")
    private IOBufferType outputBufferType;

    @JsonProperty("SplitsPerBlock")
    private Integer splitsPerBlock;

    @JsonProperty("YarnQueue")
    private String yarnQueue;

    // testing via real time proxy
    @JsonProperty("UseRealTimeProxy")
    private Boolean useRealTimeProxy;

    @JsonProperty("RealTimeProxyUrl")
    private String realTimeProxyUrl;

    @JsonProperty("RealTimeThreadPoolSize")
    private Integer realTimeThreadPoolSize;

    // ====================
    // BEGIN FLAGS
    // ====================

    @JsonProperty("ExcludePublicDomain")
    private Boolean excludePublicDomain;

    @JsonProperty("PublicDomainAsNormalDomain")
    private boolean publicDomainAsNormalDomain;

    @JsonProperty("FetchOnly")
    private boolean fetchOnly;

    @JsonProperty("SkipKeyResolution")
    private boolean skipKeyResolution;

    // if not provided, first check DnB cache before going to DnB api
    @JsonProperty("UseDnBCache")
    private boolean useDnBCache = true;

    // Flag useRemoteDnB decides whether go to DnB api.
    // Purpose of this flag: If feature of using fuzzy match is turned off, DnB
    // cache is used to do exact location lookup, but DnB api is not called
    @JsonProperty("UseRemoteDnB")
    private Boolean useRemoteDnB;

    // Flag logDnBBulkResult decides whether DnB bulk match result is logged
    @JsonProperty("LogDnBBulkResult")
    private boolean logDnBBulkResult;

    // Flag to add DnB columns match output file
    @JsonProperty("MatchDebugEnabled")
    private boolean matchDebugEnabled;

    private String matchResultPath;

    @JsonProperty("DisableDunsValidation")
    private boolean disableDunsValidation;

    @JsonProperty("PrepareForDedupe")
    private boolean prepareForDedupe;

    // use cascading bulk match
    @JsonProperty("BulkOnly")
    private boolean bulkOnly;

    // ====================
    // END FLAGS
    // ====================

    public MatchRequestSource getRequestSource() {
        return requestSource;
    }

    public void setRequestSource(MatchRequestSource requestSource) {
        this.requestSource = requestSource;
    }

    public boolean isUseDnBCache() {
        return useDnBCache;
    }

    public boolean isLogDnBBulkResult() {
        return logDnBBulkResult;
    }

    public Long getTimeout() {
        return timeout;
    }

    public void setTimeout(Long timeout) {
        this.timeout = timeout;
    }

    public Boolean getExcludePublicDomain() {
        return Boolean.TRUE.equals(excludePublicDomain);
    }

    public void setExcludePublicDomain(Boolean excludePublicDomain) {
        this.excludePublicDomain = Boolean.TRUE.equals(excludePublicDomain);
    }

    public boolean getFetchOnly() {
        return fetchOnly;
    }

    public void setFetchOnly(boolean fetchOnly) {
        this.fetchOnly = fetchOnly;
    }

    public boolean isPublicDomainAsNormalDomain() {
        return publicDomainAsNormalDomain;
    }

    public void setPublicDomainAsNormalDomain(boolean publicDomainAsNormalDomain) {
        this.publicDomainAsNormalDomain = publicDomainAsNormalDomain;
    }

    public boolean isSkipKeyResolution() {
        return skipKeyResolution;
    }

    public void setSkipKeyResolution(boolean skipKeyResolution) {
        this.skipKeyResolution = skipKeyResolution;
    }

    public boolean isPrepareForDedupe() {
        return prepareForDedupe;
    }

    public void setPrepareForDedupe(boolean prepareForDedupe) {
        this.prepareForDedupe = prepareForDedupe;
    }

    public String getDecisionGraph() {
        return decisionGraph;
    }

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

    public Map<MatchKey, List<String>> getKeyMap() {
        return keyMap;
    }

    public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
        this.keyMap = keyMap;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @JsonIgnore
    @MetricField(name = "InputFields", fieldType = MetricField.FieldType.INTEGER)
    public Integer getNumInputFields() {
        return getFields().size();
    }

    public List<List<Object>> getData() {
        return data;
    }

    public void setData(List<List<Object>> data) {
        this.data = data;
        setNumRows(data.size());
    }

    public InputBuffer getInputBuffer() {
        return inputBuffer;
    }

    public void setInputBuffer(InputBuffer buffer) {
        this.inputBuffer = buffer;
    }

    public IOBufferType getOutputBufferType() {
        return outputBufferType;
    }

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
    public Tenant getTenant() {
        return tenant;
    }

    public void setTenant(Tenant tenant) {
        this.tenant = tenant;
    }

    @MetricTagGroup
    public Predefined getPredefinedSelection() {
        return predefinedSelection;
    }

    public void setPredefinedSelection(Predefined predefinedSelection) {
        this.predefinedSelection = predefinedSelection;
    }

    public String getPredefinedVersion() {
        return predefinedVersion;
    }

    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public ColumnSelection getCustomSelection() {
        return customSelection;
    }

    public void setCustomSelection(ColumnSelection customSelection) {
        this.customSelection = customSelection;
    }

    public UnionSelection getUnionSelection() {
        return unionSelection;
    }

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

    public String getRootOperationUid() {
        return rootOperationUid;
    }

    public void setRootOperationUid(String rootOperationUid) {
        this.rootOperationUid = rootOperationUid;
    }

    public String getYarnQueue() {
        return yarnQueue;
    }

    public void setYarnQueue(String yarnQueue) {
        this.yarnQueue = yarnQueue;
    }

    public boolean isBulkOnly() {
        return bulkOnly;
    }

    public void setBulkOnly(boolean bulkOnly) {
        this.bulkOnly = bulkOnly;
    }

    public Boolean getUseRealTimeProxy() {
        return useRealTimeProxy;
    }

    public void setUseRealTimeProxy(Boolean useRealTimeProxy) {
        this.useRealTimeProxy = useRealTimeProxy;
    }

    public String getRealTimeProxyUrl() {
        return realTimeProxyUrl;
    }

    public void setRealTimeProxyUrl(String realTimeProxyUrl) {
        this.realTimeProxyUrl = realTimeProxyUrl;
    }

    public Integer getRealTimeThreadPoolSize() {
        return realTimeThreadPoolSize;
    }

    public void setRealTimeThreadPoolSize(Integer realTimeThreadPoolSize) {
        this.realTimeThreadPoolSize = realTimeThreadPoolSize;
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

    public void setLogDnBBulkResult(boolean logDnBBulkResult) {
        this.logDnBBulkResult = logDnBBulkResult;
    }

    public boolean isMatchDebugEnabled() {
        return matchDebugEnabled;
    }

    public void setMatchDebugEnabled(boolean matchDebugEnabled) {
        this.matchDebugEnabled = matchDebugEnabled;
    }

    public boolean isDisableDunsValidation() {
        return disableDunsValidation;
    }

    public void setDisableDunsValidation(boolean disableDunsValidation) {
        this.disableDunsValidation = disableDunsValidation;
    }

    public Integer getSplitsPerBlock() {
        return splitsPerBlock;
    }

    public void setSplitsPerBlock(Integer splitsPerBlock) {
        this.splitsPerBlock = splitsPerBlock;
    }

    @JsonProperty("MatchResultLocation")
    public String getMatchResultPath() {
        return matchResultPath;
    }

    @JsonProperty("MatchResultLocation")
    public void setMatchResultPath(String matchResultPath) {
        this.matchResultPath = matchResultPath;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public MatchInput configurationDeepCopy() {
        MatchInput deepCopy = JsonUtils.deserialize(toString(), MatchInput.class);
        deepCopy.setData(Collections.emptyList());
        deepCopy.setMatchEngine(getMatchEngine());
        deepCopy.setNumSelectedColumns(numSelectedColumns);
        return deepCopy;
    }

}
