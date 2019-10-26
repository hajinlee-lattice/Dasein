package com.latticeengines.domain.exposed.serviceflows.cdl.play;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;

public class GenerateLaunchUniverseStepConfiguration extends BaseStepConfiguration {
    private CustomerSpace customerSpace;
    private String channelId;
    private String playId;
    private String executionId;
    private DataCollection.Version version;

    public CustomerSpace getCustomerSpace() {
        return customerSpace;
    }

    public void setCustomerSpace(CustomerSpace customerSpace) {
        this.customerSpace = customerSpace;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) { this.channelId = channelId; }

    public String getPlayId() {
        return playId;
    }

    public void setPlayId(String playId) {
        this.playId = playId;
    }

    public DataCollection.Version getVersion() {
        return version;
    }

    public void setVersion(DataCollection.Version version) {
        this.version = version;
    }

    public String getExecutionId() { return executionId; }

    public void setExecutionId(String executionId) { this.executionId = executionId; }
}
