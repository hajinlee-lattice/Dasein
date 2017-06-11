package com.latticeengines.cdl.workflow.steps.match;

import java.util.Map;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.MatchConfiguration;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.match.MatchListOfEntitiesConfiguration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("matchListOfEntities")
public class MatchListOfEntities extends BaseWorkflowStep<MatchListOfEntitiesConfiguration> {
    
    @Autowired
    private MatchData matchData;

    @Override
    public void execute() {
        Map<String, MatchConfiguration> configs = configuration.getMatchConfigs();
        
        matchData.setExecutionContext(executionContext);
        for (Map.Entry<String, MatchConfiguration> entry : configs.entrySet()) {
            matchData.setConfiguration(entry.getValue());
            matchData.execute();
        }
    }

}
