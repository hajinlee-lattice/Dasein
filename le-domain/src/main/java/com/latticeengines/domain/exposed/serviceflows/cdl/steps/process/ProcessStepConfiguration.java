package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.Collections;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.datafeed.DataFeed;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ProcessStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("data_feed_status")
    private DataFeed.Status datafeedStatus;

    @JsonProperty("importa_and_delete_job_ids")
    private List<Long> importAndDeleteJobIds = Collections.emptyList();

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

    public DataFeed.Status getInitialDataFeedStatus() {
        return datafeedStatus;
    }

    public void setInitialDataFeedStatus(DataFeed.Status initialDataFeedStatus) {
        this.datafeedStatus = initialDataFeedStatus;
    }

    public List<Long> getImportAndDeleteJobIds() {
        return importAndDeleteJobIds;
    }

    public void setImportAndDeleteJobIds(List<Long> importAndDeleteJobIds) {
        this.importAndDeleteJobIds = importAndDeleteJobIds;
    }

    public List<Long> getActionIds() {
        return this.actionIds;
    }

    public void setActionIds(List<Long> actionIds) {
        this.actionIds = actionIds;
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
}
