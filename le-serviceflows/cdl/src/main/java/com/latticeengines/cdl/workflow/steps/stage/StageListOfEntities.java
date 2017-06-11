package com.latticeengines.cdl.workflow.steps.stage;

import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.stage.StageDataConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.stage.StageListOfEntitiesConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("stageListOfEntities")
public class StageListOfEntities extends BaseWorkflowStep<StageListOfEntitiesConfiguration> {
    
    @Autowired
    private StageData stageData;

    @Override
    public void execute() {
        Map<String, StageDataConfiguration> configs = configuration.getStageDataConfigs();
        
        stageData.setExecutionContext(executionContext);
        for (Map.Entry<String, StageDataConfiguration> entry : configs.entrySet()) {
            stageData.setConfiguration(entry.getValue());
            stageData.execute();
        }
    }

}
