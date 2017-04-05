package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.match.MatchConstants;
import com.latticeengines.domain.exposed.dataflow.flows.CombineMatchDebugWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineMatchDebugWithScoreDataFlow")
public class CombineMatchDebugWithScoreDataFlow extends RunDataFlow<CombineMatchDebugWithScoreDataFlowConfiguration> {

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
        configureMatchoutput();
    }

    private void setupDataFlow() {
        Table table = getObjectFromContext(EVENT_TABLE, Table.class);
        CombineMatchDebugWithScoreParameters params = new CombineMatchDebugWithScoreParameters(
                getScoreResultTableName(), table.getName());
        params.setColumnsToRetain(MatchConstants.matchDebugFields);
        configuration.setDataFlowParams(params);
    }

    private void configureMatchoutput() {
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, configuration.getTargetTableName());
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = getDataFlowParams().getScoreResultsTableName();
        }
        return scoreResultTableName;
    }

    private CombineMatchDebugWithScoreParameters getDataFlowParams() {
        return (CombineMatchDebugWithScoreParameters) configuration.getDataFlowParams();
    }

}
