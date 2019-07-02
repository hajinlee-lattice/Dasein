package com.latticeengines.domain.exposed.datacloud.match;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Level;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
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
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection.Predefined;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class MatchInput implements Fact, Dimension {

    // legacy configuration to be removed
    @JsonIgnore
    private static final String predefinedVersion = "2.0";
    @JsonProperty("Tenant")
    private Tenant tenant;
    @JsonProperty("Fields")
    private List<String> fields;
    // IMPORTANT: Number of records cannot exceed 200
    @JsonProperty("Data")
    private List<List<Object>> data;
    @JsonIgnore
    private int numRows;

    // For ldc match:
    // If decisionGraph is empty, will load default ldc decision graph
    // For entity match:
    // Default behavior: set targetEntity and leave decisionGraph as empty and
    // will
    // use targetEntity's default decision graph.
    // Also allow setting decisionGraph but leave targetEntity as empty, will
    // use decisionGraph's corresponding entity as targetEntity.
    // If both decisionGraph and targetEntity are set, will
    // validate whether decisionGraph and targetEntity are matched. If not
    // matched, fail the match request/job.
    @JsonProperty("DecisionGraph")
    private String decisionGraph;

    @JsonProperty("LogLevel")
    private String logLevel;
    @JsonProperty("RootOperationUid")
    private String rootOperationUid;
    @JsonIgnore
    private String matchEngine;
    // optional, but better to provide. if not, will be resolved from the fields
    @JsonProperty("KeyMap")
    private Map<MatchKey, List<String>> keyMap;
    @JsonProperty("LookupId")
    private String lookupId;
    // =========================
    // BEGIN: column selections
    // =========================
    // only one of these is needed, custom selection has higher priority
    @JsonProperty("PredefinedSelection")
    private Predefined predefinedSelection;
    @JsonProperty("CustomSelection")
    private ColumnSelection customSelection;
    @JsonProperty("UnionSelection")
    private UnionSelection unionSelection;
    @JsonIgnore
    private List<ColumnMetadata> metadatas;

    // =========================
    // END: column selections
    // =========================
    @JsonIgnore
    private List<String> metadataFields;
    @JsonProperty("TimeOut")
    private Long timeout;
    @JsonProperty("RequestSource")
    private MatchRequestSource requestSource = MatchRequestSource.SCORING;
    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion;

    // only used by cdl lookup
    @JsonProperty("DataCollectionVersion")
    private DataCollection.Version dataCollectionVersion;

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

    // Must provide LatticeAccountId (datacloud match) or EntityId (entity
    // match)
    // Actor system will be skipped. Directly fetch datacloud/entity seed
    // attributes based on ID provided
    @JsonProperty("FetchOnly")
    private boolean fetchOnly;

    // only match to LDC even for a CDL tenant
    @JsonProperty("DataCloudOnly")
    private Boolean dataCloudOnly;

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

    @JsonProperty("PartialMatchEnabled")
    private boolean partialMatchEnabled;

    // ====================
    // END FLAGS
    // ====================

    // ====================
    // BEGIN ENTITY MATCH PROPERTIES
    // ====================

    @JsonProperty("OperationalMode")
    private OperationalMode operationalMode;

    // Allocate ID only applies in ENTITY_MATCH Operational Mode.  Further, it refers to an Entity Match process
    // where an ID can be created, rather than just looked up.  This option is only available in Bulk Match mode,
    // thus allocateId must be false for Real Time match.
    @JsonProperty("AllocateId")
    private boolean allocateId;

    // Target entity represents the ultimate Business Entity that this match request is trying to find an ID for.
    // Since some entity matches will require matching other entities, eg. Contact might require Account, and
    // Transaction requires Account, Contact, and Product, it won't necessarily be clear from the Entity Key Map
    // what the ultimate match goal is.
    // TargetEntity = LatticeAccount means it's LDC match (default)
    @JsonProperty("TargetEntity")
    private String targetEntity = BusinessEntity.LatticeAccount.name();

    // A map from Business Entity (as a String) to EntityKeyMap.
    @JsonProperty("EntityKeyMaps")
    private Map<String, EntityKeyMap> entityKeyMaps;

    // Temporary flag to output all newly allocated entities to avro files. Only
    // applies to bulk match with allocateId mode.
    // TODO: change this after solution is finalized
    @JsonProperty("OutputNewEntities")
    private boolean outputNewEntities;

    // Temporary flag for entity bulk match test. Will remove after we have
    // workflow for testing
    @JsonProperty("BumpupEntitySeedVersion")
    private boolean bumpupEntitySeedVersion;

    // ====================
    // END ENTITY MATCH PROPERTIES
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

    public void setUseDnBCache(boolean useDnBCache) {
        this.useDnBCache = useDnBCache;
    }

    public boolean isLogDnBBulkResult() {
        return logDnBBulkResult;
    }

    public void setLogDnBBulkResult(boolean logDnBBulkResult) {
        this.logDnBBulkResult = logDnBBulkResult;
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

    public boolean isFetchOnly() {
        return fetchOnly;
    }

    public void setFetchOnly(boolean fetchOnly) {
        this.fetchOnly = fetchOnly;
    }

    public Boolean getDataCloudOnly() {
        return dataCloudOnly;
    }

    public void setDataCloudOnly(Boolean dataCloudOnly) {
        this.dataCloudOnly = dataCloudOnly;
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
    public Level getLogLevelEnum() {
        if (StringUtils.isNotEmpty(logLevel)) {
            return Level.toLevel(logLevel);
        } else {
            return null;
        }
    }

    @JsonIgnore
    public void setLogLevelEnum(Level logLevel) {
        if (logLevel == null) {
            this.logLevel = null;
        } else {
            this.logLevel = logLevel.toString();
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

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
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

    public List<ColumnMetadata> getMetadatas() {
        return metadatas;
    }

    public void setMetadatas(List<ColumnMetadata> metadatas) {
        this.metadatas = metadatas;
    }

    public List<String> getMetadataFields() {
        return metadataFields;
    }

    public void setMetadataFields(List<String> metadataFields) {
        this.metadataFields = metadataFields;
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

    public boolean isPartialMatchEnabled() {
        return partialMatchEnabled;
    }

    public void setPartialMatchEnabled(boolean partialMatchEnabled) {
        this.partialMatchEnabled = partialMatchEnabled;
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

    public Boolean getUseRemoteDnB() {
        return useRemoteDnB;
    }

    public void setUseRemoteDnB(Boolean useRemoteDnB) {
        this.useRemoteDnB = useRemoteDnB;
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

    public OperationalMode getOperationalMode() {
        return operationalMode;
    }

    public void setOperationalMode(OperationalMode operationalMode) {
        this.operationalMode = operationalMode;
    }

    public boolean isAllocateId() {
        return allocateId;
    }

    public void setAllocateId(boolean allocateId) {
        this.allocateId = allocateId;
    }

    public String getTargetEntity() {
        return targetEntity;
    }

    public void setTargetEntity(String targetEntity) {
        this.targetEntity = targetEntity;
    }

    public Map<String, EntityKeyMap> getEntityKeyMaps() {
        return entityKeyMaps;
    }

    public void setEntityKeyMaps(Map<String, EntityKeyMap> entityKeyMaps) {
        this.entityKeyMaps = entityKeyMaps;
    }

    public boolean isOutputNewEntities() {
        return outputNewEntities;
    }

    public void setOutputNewEntities(boolean outputNewEntities) {
        this.outputNewEntities = outputNewEntities;
    }

    public boolean bumpupEntitySeedVersion() {
        return bumpupEntitySeedVersion;
    }

    public void setBumpupEntitySeedVersion(boolean bumpupEntitySeedVersion) {
        this.bumpupEntitySeedVersion = bumpupEntitySeedVersion;
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

    public static class EntityKeyMap {

        // MatchKey -> mapped fields (prioritized)
        // SystemId MatchKey: the priority is decided by mapped fields order
        // Domain MatchKey: choose 1st mapped field whose domain value is
        // not empty and not public domain
        @JsonProperty("KeyMap")
        private Map<MatchKey, List<String>> keyMap;

        public EntityKeyMap() {
        }

        public EntityKeyMap(Map<MatchKey, List<String>> keyMap) {
            this.keyMap = keyMap;
        }

        public Map<MatchKey, List<String>> getKeyMap() {
            return keyMap;
        }

        public static EntityKeyMap fromKeyMap(Map<MatchKey, List<String>> keyMap) {
            EntityKeyMap entityKeyMap = new EntityKeyMap();
            entityKeyMap.setKeyMap(keyMap);
            return entityKeyMap;
        }

        public void setKeyMap(Map<MatchKey, List<String>> keyMap) {
            this.keyMap = keyMap;
        }

        public void addMatchKey(MatchKey key, String attr) {
            if (MapUtils.isEmpty(keyMap)) {
                keyMap = new HashMap<>();
            }
            keyMap.computeIfAbsent(key, k -> new ArrayList<>());
            keyMap.get(key).add(attr);
        }

    }


}
