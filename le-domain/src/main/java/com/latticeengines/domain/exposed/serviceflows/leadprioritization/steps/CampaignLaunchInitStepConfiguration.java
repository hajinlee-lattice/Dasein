package com.latticeengines.domain.exposed.serviceflows.leadprioritization.steps;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class CampaignLaunchInitStepConfiguration extends BaseStepConfiguration {
    private CustomerSpace customerSpace;
    private String playName;
    private String playLaunchId;

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
}
