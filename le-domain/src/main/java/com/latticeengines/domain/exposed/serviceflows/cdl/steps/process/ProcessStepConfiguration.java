package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;
import com.latticeengines.domain.exposed.workflow.WorkflowContextConstants;

public class ProcessStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_status")
    private DataFeed.Status datafeedStatus;

    @JsonProperty("action_ids")
    private List<Long> actionIds = Collections.emptyList();

    @JsonProperty("data_cloud_build_number")
    private String dataCloudBuildNumber;

    @JsonProperty("ignore_data_cloud_change")
    private Boolean ignoreDataCloudChange;

    @JsonProperty("aps_rolling_period")
    private String apsRollingPeriod;

    @JsonProperty("user_id")
    private String userId;

    @JsonProperty("owner_id")
    private long ownerId;

    @JsonProperty("skip_entities")
    private Set<BusinessEntity> skipEntities;

    @JsonProperty("rebuild_entities")
    private Set<BusinessEntity> rebuildEntities;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @JsonProperty("full_rematch")
    private boolean fullRematch;

    @JsonProperty("auto_schedule")
    private boolean autoSchedule;

    @JsonProperty("input_properties")
    private Map<String, String> inputProperties;

    @JsonProperty("skip_publish_to_s3")
    private boolean skipPublishToS3;

    private boolean targetScoreDerivationEnabled;

    public DataFeed.Status getInitialDataFeedStatus() {
        return datafeedStatus;
    }

    public void setInitialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
        this.datafeedStatus = initialDataFeedStatus;
    }

    public List<Long> getActionIds() {
        return this.actionIds;
    }

    public void setActionIds(List<Long> actionIds) {
        this.actionIds = actionIds;
        if (this.inputProperties != null) {
            syncActionIds();
        }
    }

    public String getDataCloudBuildNumber() {
        return dataCloudBuildNumber;
    }

    public void setDataCloudBuildNumber(String dataCloudBuildNumber) {
        this.dataCloudBuildNumber = dataCloudBuildNumber;
    }

    public Boolean getIgnoreDataCloudChange() {
        return ignoreDataCloudChange;
    }

    public void setIgnoreDataCloudChange(Boolean ignoreDataCloudChange) {
        this.ignoreDataCloudChange = ignoreDataCloudChange;
    }

    public String getApsRollingPeriod() {
        return apsRollingPeriod;
    }

    public void setApsRollingPeriod(String apsRollingPeriod) {
        this.apsRollingPeriod = apsRollingPeriod;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public long getOwnerId() {
        return this.ownerId;
    }

    public void setOwnerId(long ownerId) {
        this.ownerId = ownerId;
    }

    public Set<BusinessEntity> getSkipEntities() {
        return skipEntities;
    }

    public void setSkipEntities(Set<BusinessEntity> skipEntities) {
        this.skipEntities = skipEntities;
    }

    public Set<BusinessEntity> getRebuildEntities() {
        return rebuildEntities;
    }

    public void setRebuildEntities(Set<BusinessEntity> rebuildEntities) {
        this.rebuildEntities = rebuildEntities;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }

    public boolean isTargetScoreDerivationEnabled() {
        return targetScoreDerivationEnabled;
    }

    public void setTargetScoreDerivationEnabled(boolean targetScoreDerivationEnabled) {
        this.targetScoreDerivationEnabled = targetScoreDerivationEnabled;
    }

    public void setTargetScoreDerivation(boolean targetScoreDerivationEnabled) {
        this.targetScoreDerivationEnabled = targetScoreDerivationEnabled;
    }
    
    public boolean isFullRematch() {
        return fullRematch;
    }

    public void setFullRematch(boolean fullRematch) {
        this.fullRematch = fullRematch;
    }

    public boolean isAutoSchedule() {
        return autoSchedule;
    }

    public void setAutoSchedule(boolean autoSchedule) {
        this.autoSchedule = autoSchedule;
    }

    public Map<String, String> getInputProperties() {
        return inputProperties;
    }

    public void setInputProperties(Map<String, String> inputProperties) {
        this.inputProperties = inputProperties;
        syncActionIds();
    }

    private void syncActionIds() {
        Set<Long> actionIdArray = new HashSet<>();
        actionIdArray.addAll(actionIds);
        String actionIdStr = this.inputProperties.get(WorkflowContextConstants.Inputs.ACTION_IDS);
        if (StringUtils.isNotEmpty(actionIdStr)) {
            List<?> rawActionIds = JsonUtils.deserialize(actionIdStr, List.class);
            actionIdArray.addAll(JsonUtils.convertList(rawActionIds, Long.class));
        }
        inputProperties.put(WorkflowContextConstants.Inputs.ACTION_IDS, JsonUtils.serialize(actionIdArray));
    }

    public boolean isSkipPublishToS3() {
        return skipPublishToS3;
    }

    public void setSkipPublishToS3(boolean skipPublishToS3) {
        this.skipPublishToS3 = skipPublishToS3;
    }
}
