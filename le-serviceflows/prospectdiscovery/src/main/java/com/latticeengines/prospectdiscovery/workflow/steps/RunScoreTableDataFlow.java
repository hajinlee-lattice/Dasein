package com.latticeengines.prospectdiscovery.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.dataflow.CreateScoreTableParameters;
import com.latticeengines.domain.exposed.serviceflows.prospectdiscovery.steps.RunScoreTableDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("runScoreTableDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RunScoreTableDataFlow extends RunDataFlow<RunScoreTableDataFlowConfiguration> {

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
        putStringValueInContext(EVENT_TABLE, configuration.getTargetTableName());
        putStringValueInContext(ATTR_LEVEL_TYPE, "AVG");
    }

    private void setupDataFlow() {
        Map<String, String> map = new HashMap<>();
        String[] acctMasterAndPath = configuration.getAccountMasterAndPath();
        map.put(acctMasterAndPath[0], acctMasterAndPath[1]);
        configuration.setExtraSources(map);

        CreateScoreTableParameters params = //
                new CreateScoreTableParameters(getScoreResultTable(), acctMasterAndPath[0],
                        configuration.getUniqueKeyColumn());
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
