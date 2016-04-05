package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.CreateScoreTableParameters;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("runScoreTableDataFlow")
public class RunScoreTableDataFlow extends RunDataFlow<RunScoreTableDataFlowConfiguration> {
    
    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
        executionContext.putString(EVENT_TABLE, configuration.getTargetTableName());
        executionContext.putString(ATTR_LEVEL_TYPE, "AVG");
    }
    
    private void setupDataFlow() {
        Map<String, String> map = new HashMap<>();
        String[] acctMasterAndPath = configuration.getAccountMasterAndPath();
        map.put(acctMasterAndPath[0], acctMasterAndPath[1]);
        configuration.setExtraSources(map);
        
        CreateScoreTableParameters params = //
                new CreateScoreTableParameters(getScoreResultTable(), acctMasterAndPath[0], configuration.getUniqueKeyColumn());
        configuration.setDataFlowParams(params);
    }
    
    private String getScoreResultTable() {
        String scoreResultTable = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTable == null) {
            scoreResultTable = configuration.getScoreResult();
        }
        return scoreResultTable;
    }


}
