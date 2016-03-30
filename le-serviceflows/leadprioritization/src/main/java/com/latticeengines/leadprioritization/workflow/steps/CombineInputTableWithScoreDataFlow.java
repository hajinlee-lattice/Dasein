package com.latticeengines.leadprioritization.workflow.steps;

import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineInputTableWithScoreDataFlow")
public class CombineInputTableWithScoreDataFlow extends RunDataFlow<CombineInputTableWithScoreDataFlowConfiguration> {

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
    }

    private void setupDataFlow() {
        CombineInputTableWithScoreParameters params = //
        new CombineInputTableWithScoreParameters(getScoreResultTableName(), getInputTableName());
        configuration.setDataFlowParams(params);
    }

    private String getInputTableName() {
        Table eventTable = JsonUtils.deserialize(getStringValueFromContext(EVENT_TABLE), Table.class);
        if (eventTable == null) {
            return configuration.getInputTableName();
        }
        return eventTable.getName();
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = configuration.getScoreResultTableName();
        }
        return scoreResultTableName;
    }
}
