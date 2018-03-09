package com.latticeengines.prospectdiscovery.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunAttributeLevelSummaryDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("runAttributeLevelSummaryDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
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
