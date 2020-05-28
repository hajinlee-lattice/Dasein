package com.latticeengines.domain.exposed.cdl;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ChoreographerContext {

    @JsonProperty
    private Set<BusinessEntity> entitiesRebuildDueToActions = new HashSet<>();

    @JsonProperty
    private boolean dataCloudChanged;

    @JsonProperty
    private boolean dataCloudRefresh;

    @JsonProperty
    private boolean dataCloudNew;

    @JsonProperty
    private boolean hasAccountAttrLifeCycleChange;

    @JsonProperty
    private boolean hasContactAttrLifeCycleChange;

    @JsonProperty
    private boolean hasRatingEngineChange;

    @JsonProperty
    private boolean purchaseMetricsChanged;

    @JsonProperty
    private boolean businessCalenderChanged;

    @JsonProperty
    private boolean fullRematch;

    @JsonProperty
    private boolean entityMatchEnabled;

    @JsonProperty
    private boolean hasSoftDelete;

    @JsonProperty
    private boolean hasAccountBatchStore;

    @JsonProperty
    private boolean hasContactBatchStore;

    @JsonProperty
    private boolean hasTransactionRawStore;

    @JsonProperty
    private boolean alwaysRebuildServingStores;

    /*-
     * temp flag for tenants to skip force rebuild PA to migrate off CustomerAccountId
     * TODO remove after all tenants are migrated off CustomerAccountId
     */
    @JsonProperty
    private boolean skipForceRebuildTxn;

    public Set<BusinessEntity> getEntitiesRebuildDueToActions() {
        return entitiesRebuildDueToActions;
    }

    public void setEntitiesRebuildDueToActions(Set<BusinessEntity> entitiesRebuildDueToActions) {
        this.entitiesRebuildDueToActions = entitiesRebuildDueToActions;
    }

    public boolean isDataCloudChanged() {
        return dataCloudChanged;
    }

    public void setDataCloudChanged(boolean dataCloudChanged) {
        this.dataCloudChanged = dataCloudChanged;
    }

    public boolean isDataCloudRefresh() {
        return dataCloudRefresh;
    }

    public void setDataCloudRefresh(boolean dataCloudRefresh) {
        this.dataCloudRefresh = dataCloudRefresh;
    }

    public boolean isDataCloudNew() {
        return dataCloudNew;
    }

    public void setDataCloudNew(boolean dataCloudNew) {
        this.dataCloudNew = dataCloudNew;
    }

    public boolean isHasAccountAttrLifeCycleChange() {
        return hasAccountAttrLifeCycleChange;
    }

    public void setHasAccountAttrLifeCycleChange(boolean hasAccountAttrLifeCycleChange) {
        this.hasAccountAttrLifeCycleChange = hasAccountAttrLifeCycleChange;
    }

    public boolean isHasContactAttrLifeCycleChange() {
        return hasContactAttrLifeCycleChange;
    }

    public void setHasContactAttrLifeCycleChange(boolean hasContactAttrLifeCycleChange) {
        this.hasContactAttrLifeCycleChange = hasContactAttrLifeCycleChange;
    }

    public boolean isHasRatingEngineChange() {
        return hasRatingEngineChange;
    }

    public void setHasRatingEngineChange(boolean hasRatingEngineChange) {
        this.hasRatingEngineChange = hasRatingEngineChange;
    }

    public boolean isPurchaseMetricsChanged() {
        return purchaseMetricsChanged;
    }

    public void setPurchaseMetricsChanged(boolean purchaseMetricsChanged) {
        this.purchaseMetricsChanged = purchaseMetricsChanged;
    }

    public boolean isBusinessCalenderChanged() {
        return businessCalenderChanged;
    }

    public void setBusinessCalenderChanged(boolean businessCalenderChanged) {
        this.businessCalenderChanged = businessCalenderChanged;
    }

    public boolean isFullRematch() {
        return fullRematch;
    }

    public void setFullRematch(boolean fullRematch) {
        this.fullRematch = fullRematch;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }

    public boolean isHasSoftDelete() {
        return hasSoftDelete;
    }

    public void setHasSoftDelete(boolean hasSoftDelete) {
        this.hasSoftDelete = hasSoftDelete;
    }

    public boolean isHasAccountBatchStore() {
        return hasAccountBatchStore;
    }

    public void setHasAccountBatchStore(boolean hasAccountBatchStore) {
        this.hasAccountBatchStore = hasAccountBatchStore;
    }

    public boolean isHasContactBatchStore() {
        return hasContactBatchStore;
    }

    public void setHasContactBatchStore(boolean hasContactBatchStore) {
        this.hasContactBatchStore = hasContactBatchStore;
    }

    public boolean isHasTransactionRawStore() {
        return hasTransactionRawStore;
    }

    public void setHasTransactionRawStore(boolean hasTransactionRawStore) {
        this.hasTransactionRawStore = hasTransactionRawStore;
    }

    public boolean isAlwaysRebuildServingStores() {
        return alwaysRebuildServingStores;
    }

    public void setAlwaysRebuildServingStores(boolean alwaysRebuildServingStores) {
        this.alwaysRebuildServingStores = alwaysRebuildServingStores;
    }

    public boolean isSkipForceRebuildTxn() {
        return skipForceRebuildTxn;
    }

    public void setSkipForceRebuildTxn(boolean skipForceRebuildTxn) {
        this.skipForceRebuildTxn = skipForceRebuildTxn;
    }
}
