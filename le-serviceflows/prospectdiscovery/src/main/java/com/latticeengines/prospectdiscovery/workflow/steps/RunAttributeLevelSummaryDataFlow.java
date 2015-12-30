package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("runAttributeLevelSummaryDataFlow")
public class RunAttributeLevelSummaryDataFlow extends RunDataFlow<RunAttributeLevelSummaryDataFlowConfiguration> {
    
    public void setConfiguration(RunAttributeLevelSummaryDataFlowConfiguration configuration) {
        this.configuration = configuration;
    }
    
    @Override
    public boolean setup() {
        restTemplate.setInterceptors(addMagicAuthHeaders);
        return true;
    }
}
