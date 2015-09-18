package com.latticeengines.domain.exposed.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;

public class DataFlowConfiguration extends BasePayloadConfiguration {

    private String dataFlowBeanName;

    @JsonProperty("dataflow_bean")
    public String getDataFlowBeanName() {
        return dataFlowBeanName;
    }

    @JsonProperty("dataflow_bean")
    public void setDataFlowBeanName(String dataFlowBeanName) {
        this.dataFlowBeanName = dataFlowBeanName;
    }
}
