package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class QueuePlayLaunchesStepConfiguration extends BaseStepConfiguration {
    private CustomerSpace customerSpace;
    private String channelId;
    private String playId;

    public String getChannelId() { return channelId; }

    public void setChannelId(String channelId) { this.channelId = channelId; }

    public String getPlayId() { return playId; }

    public void setPlayId(String playId) { this.playId = playId; }

    public CustomerSpace getCustomerSpace() { return customerSpace; }

    public void setCustomerSpace(CustomerSpace customerSpace) { this.customerSpace = customerSpace; }
}
