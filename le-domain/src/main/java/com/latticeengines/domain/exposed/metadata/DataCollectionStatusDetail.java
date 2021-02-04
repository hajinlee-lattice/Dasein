package com.latticeengines.domain.exposed.metadata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.cdl.activity.ActivityBookkeeping;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class DataCollectionStatusDetail implements Serializable {
    private static final long serialVersionUID = 7817179046757931427L;

    private static final TypeReference<Map<String, AtlasStream>> STREAM_MAP_TYPE = new TypeReference<Map<String, AtlasStream>>() {
    };

    public static final String NOT_SET = "not set";

    @JsonProperty("DateMap")
    private Map<String, Long> dateMap;

    // catalogName -> list of original file name used to build catalog store
    @JsonProperty("OrigCatalogFileMap")
    private Map<String, List<String>> origCatalogFileMap;

    // JSON str of Map[ streamId -> stream object ]
    @JsonProperty("ActivityStreamMapStr")
    private String activityStreamMapStr;

    // streamId -> dateId
    @JsonProperty("ActivityStreamLastRefresh")
    private Map<String, Integer> activityStreamLastRefresh;

    @JsonProperty("DimensionMetadataSignature")
    private String dimensionMetadataSignature;

    @JsonProperty("MinTxnDate")
    private Integer minTxnDate = 0;

    @JsonProperty("MaxTxnDate")
    private Integer maxTxnDate = 0;

    @JsonProperty("EvaluationDate")
    private String evaluationDate = NOT_SET;

    @JsonProperty("DataCloudBuildNumber")
    private String dataCloudBuildNumber = NOT_SET;

    @JsonProperty("AccountCount")
    private Long accountCount = 0L;

    @JsonProperty("ContactCount")
    private Long contactCount = 0L;

    @JsonProperty("TransactionCount")
    private Long transactionCount = 0L;

    @JsonProperty("ProductCount")
    private Long productCount = 0L;

    @JsonProperty("OrphanContactCount")
    private Long orphanContactCount = 0L;

    @JsonProperty("OrphanTransactionCount")
    private Long orphanTransactionCount = 0L;

    @JsonProperty("UnmatchedAccountCount")
    private Long unmatchedAccountCount = 0L;

    @JsonProperty("ApsRollingPeriod")
    private String apsRollingPeriod;

    @JsonProperty("ServingStoreVersion")
    private int servingStoreVersion;

    @JsonProperty("RedshiftPartition")
    private String redshiftPartition;

    /*-
     * temp flag to track whether a tenant's transaction store has been migrated off CustomerAccountId
     * TODO remove after everyone is migrated
     */
    @JsonProperty("TransactionRebuilt")
    private Boolean transactionRebuilt;

    /*
     * temp flag to indicate a tenant's txn is built with new steps TODO remove
     * after new steps support incremental update
     */
    @JsonProperty("TransactionRebuiltWithNewSteps")
    private Boolean transactionRebuiltWithNewSteps;

    @JsonProperty("activityBookKeeping")
    private ActivityBookkeeping bookkeeping;

    // key: timelineId -> value: timelineVersion
    @JsonProperty("timelineVersionMap")
    private Map<String, String> timelineVersionMap;

    @JsonProperty("activityPartitionKeyMigrated")
    private boolean activityPartitionKeyMigrated = false;

    @JsonProperty("timelineRebuildFlag")
    private Boolean timelineRebuildFlag;

    // maybe retire as value has been stored in data unit
    //key: BusinessEntity.name() -> value: elasticsearchVersion
    @JsonProperty("entityToESVersionMap")
    private Map<String, String> entityToESVersionMap;

    @JsonProperty("ActivityAlertVersion")
    private String activityAlertVersion;

    @JsonProperty("IntentAlertVersion")
    private String intentAlertVersion;

    @JsonProperty("spendingAnalysisPublished")
    private Boolean spendingAnalysisPublished;

    /*-
     * general key -> epoch timestamp map where some data/metric/stat is calculated
     * TODO consider just use table role as key
     *
     * separated from dateMap which is currently using Category enum as key
     */
    @JsonProperty("EvaluationDateMap")
    private Map<String, Long> evaluationDateMap;

    @JsonProperty("accountLookupSource")
    private List<String> accountLookupSource = new ArrayList<>();

    public Integer getMinTxnDate() {
        return minTxnDate;
    }

    public void setMinTxnDate(Integer minTxnDate) {
        this.minTxnDate = minTxnDate;
    }

    public Integer getMaxTxnDate() {
        return maxTxnDate;
    }

    public void setMaxTxnDate(Integer maxTxnDate) {
        this.maxTxnDate = maxTxnDate;
    }

    public String getEvaluationDate() {
        return evaluationDate;
    }

    public void setEvaluationDate(String evaluationDate) {
        this.evaluationDate = evaluationDate;
    }

    public String getDataCloudBuildNumber() {
        return dataCloudBuildNumber;
    }

    public void setDataCloudBuildNumber(String dataCloudBuildNumber) {
        this.dataCloudBuildNumber = dataCloudBuildNumber;
    }

    public Long getAccountCount() {
        return accountCount;
    }

    public void setAccountCount(Long accountCount) {
        this.accountCount = accountCount;
    }

    public Long getContactCount() {
        return contactCount;
    }

    public void setContactCount(Long contactCount) {
        this.contactCount = contactCount;
    }

    public Long getTransactionCount() {
        return transactionCount;
    }

    public void setTransactionCount(Long transactionCount) {
        this.transactionCount = transactionCount;
    }

    public Long getProductCount() {
        return productCount;
    }

    public void setProductCount(Long productCount) {
        this.productCount = productCount;
    }

    public Long getOrphanContactCount() {
        return orphanContactCount;
    }

    public void setOrphanContactCount(Long orphanContactCount) {
        this.orphanContactCount = orphanContactCount;
    }

    public Long getOrphanTransactionCount() {
        return orphanTransactionCount;
    }

    public void setOrphanTransactionCount(Long orphanTransactionCount) {
        this.orphanTransactionCount = orphanTransactionCount;
    }

    public Long getUnmatchedAccountCount() {
        return unmatchedAccountCount;
    }

    public void setUnmatchedAccountCount(Long unmatchedAccountCount) {
        this.unmatchedAccountCount = unmatchedAccountCount;
    }

    public String getApsRollingPeriod() {
        return apsRollingPeriod;
    }

    public void setApsRollingPeriod(String apsRollingPeriod) {
        this.apsRollingPeriod = apsRollingPeriod;
    }

    public Map<String, Long> getDateMap() {
        return dateMap;
    }

    public void setDateMap(Map<String, Long> dateMap) {
        this.dateMap = dateMap;
    }

    public Map<String, List<String>> getOrigCatalogFileMap() {
        return origCatalogFileMap;
    }

    public void setOrigCatalogFileMap(Map<String, List<String>> origCatalogFileMap) {
        this.origCatalogFileMap = origCatalogFileMap;
    }

    public Map<String, AtlasStream> getActivityStreamMap() {
        if (activityStreamMapStr == null) {
            return Collections.emptyMap();
        }

        return JsonUtils.deserialize(activityStreamMapStr, STREAM_MAP_TYPE);
    }

    public void setActivityStreamMap(Map<String, AtlasStream> activityStreamMap) {
        this.activityStreamMapStr = JsonUtils.serialize(activityStreamMap);
    }

    public String getDimensionMetadataSignature() {
        return dimensionMetadataSignature;
    }

    public void setDimensionMetadataSignature(String dimensionMetadataSignature) {
        this.dimensionMetadataSignature = dimensionMetadataSignature;
    }

    public int getServingStoreVersion() {
        return servingStoreVersion;
    }

    public void setServingStoreVersion(int servingStoreVersion) {
        this.servingStoreVersion = servingStoreVersion;
    }

    public String getRedshiftPartition() {
        return redshiftPartition;
    }

    public void setRedshiftPartition(String redshiftPartition) {
        this.redshiftPartition = redshiftPartition;
    }

    public Boolean getTransactionRebuilt() {
        return transactionRebuilt;
    }

    public void setTransactionRebuilt(Boolean transactionRebuilt) {
        this.transactionRebuilt = transactionRebuilt;
    }

    public Boolean getTransactionRebuiltWithNewSteps() {
        return transactionRebuiltWithNewSteps;
    }

    public void setTransactionRebuiltWithNewSteps(Boolean transactionRebuiltWithNewSteps) {
        this.transactionRebuiltWithNewSteps = transactionRebuiltWithNewSteps;
    }

    public ActivityBookkeeping getBookkeeping() {
        return bookkeeping;
    }

    public void setBookkeeping(ActivityBookkeeping bookkeeping) {
        this.bookkeeping = bookkeeping;
    }

    public Map<String, String> getTimelineVersionMap() {
        return timelineVersionMap;
    }

    public void setTimelineVersionMap(Map<String, String> timelineVersionMap) {
        this.timelineVersionMap = timelineVersionMap;
    }

    public boolean isActivityPartitionKeyMigrated() {
        return activityPartitionKeyMigrated;
    }

    public void setActivityPartitionKeyMigrated(boolean activityPartitionKeyMigrated) {
        this.activityPartitionKeyMigrated = activityPartitionKeyMigrated;
    }

    public Map<String, Integer> getActivityStreamLastRefresh() {
        return activityStreamLastRefresh;
    }

    public void setActivityStreamLastRefresh(Map<String, Integer> activityStreamLastRefresh) {
        this.activityStreamLastRefresh = activityStreamLastRefresh;
    }

    public Boolean isTimelineRebuildFlag() {
        return timelineRebuildFlag;
    }

    public void setTimelineRebuildFlag(Boolean timelineRebuildFlag) {
        this.timelineRebuildFlag = timelineRebuildFlag;
    }

    public Map<String, String> getEntityToESVersionMap() {
        return entityToESVersionMap;
    }

    public void setEntityToESVersionMap(Map<String, String> entityToESVersionMap) {
        this.entityToESVersionMap = entityToESVersionMap;
    }

    public String getActivityAlertVersion() {
        return activityAlertVersion;
    }

    public void setActivityAlertVersion(String activityAlertVersion) {
        this.activityAlertVersion = activityAlertVersion;
    }

    public String getIntentAlertVersion() {
        return intentAlertVersion;
    }

    public void setIntentAlertVersion(String intentAlertVersion) {
        this.intentAlertVersion = intentAlertVersion;
    }

    public List<String> getAccountLookupSource() {
        return accountLookupSource;
    }

    public void setAccountLookupSource(List<String> accountLookupSource) {
        this.accountLookupSource = accountLookupSource;
    }

    public Map<String, Long> getEvaluationDateMap() {
        return evaluationDateMap;
    }

    public void setEvaluationDateMap(Map<String, Long> evaluationDateMap) {
        this.evaluationDateMap = evaluationDateMap;
    }

    public Boolean getSpendingAnalysisPublished() {
        return spendingAnalysisPublished;
    }

    public void setSpendingAnalysisPublished(Boolean spendingAnalysisPublished) {
        this.spendingAnalysisPublished = spendingAnalysisPublished;
    }
}
