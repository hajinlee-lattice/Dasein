package com.latticeengines.leadprioritization.workflow.steps;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.dataflow.flows.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineInputTableWithScoreDataFlow")
public class CombineInputTableWithScoreDataFlow extends RunDataFlow<CombineInputTableWithScoreDataFlowConfiguration> {

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
        configureExport();
    }

    private void setupDataFlow() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters(
                getScoreResultTableName(), getInputTableName(), getBucketMetadata());
        configuration.setDataFlowParams(params);
    }

    private void configureExport() {
        putStringValueInContext(EXPORT_TABLE_NAME, configuration.getTargetTableName());
    }

    private String getInputTableName() {
        return getDataFlowParams().getInputTableName();
    }

    private String getScoreResultTableName() {
        String scoreResultTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);
        if (scoreResultTableName == null) {
            scoreResultTableName = getDataFlowParams().getScoreResultsTableName();
        }
        return scoreResultTableName;
    }

    private CombineInputTableWithScoreParameters getDataFlowParams() {
        return (CombineInputTableWithScoreParameters) configuration.getDataFlowParams();
    }

    private List<BucketMetadata> getBucketMetadata() {
        return getDataFlowParams().getBucketMetadata();
    }
}
