package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CampaignLaunchInitStepConfiguration extends BaseStepConfiguration {

    @JsonProperty("customer_space")
    private CustomerSpace customerSpace;

    @JsonProperty("play_name")
    private String playName;

    @JsonProperty("play_launch_id")
    private String playLaunchId;

    @JsonProperty("data_collection_version")
    private DataCollection.Version dataCollectionVersion;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getPlayName() {
        return playName;
    }

    public void setPlayName(String playName) {
        this.playName = playName;
    }

    public String getPlayLaunchId() {
        return playLaunchId;
    }

    public void setPlayLaunchId(String playLaunchId) {
        this.playLaunchId = playLaunchId;
    }

    public DataCollection.Version getDataCollectionVersion() {
        return dataCollectionVersion;
    }

    public void setDataCollectionVersion(DataCollection.Version dataCollectionVersion) {
        this.dataCollectionVersion = dataCollectionVersion;
    }
}
