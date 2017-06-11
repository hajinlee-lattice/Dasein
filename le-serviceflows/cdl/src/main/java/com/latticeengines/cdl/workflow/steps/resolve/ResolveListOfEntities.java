package com.latticeengines.cdl.workflow.steps.resolve;

import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.resolve.ResolveDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.resolve.ResolveListOfEntitiesConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("resolveListOfEntities")
public class ResolveListOfEntities extends BaseWorkflowStep<ResolveListOfEntitiesConfiguration> {
    
    @Autowired
    private ResolveData resolveData;

    @Override
    public void execute() {
        Map<String, ResolveDataConfiguration> configs = configuration.getResolveDataConfigs();
        
        resolveData.setExecutionContext(executionContext);
        for (Map.Entry<String, ResolveDataConfiguration> entry : configs.entrySet()) {
            resolveData.setConfiguration(entry.getValue());
            resolveData.execute();
        }
    }

}
