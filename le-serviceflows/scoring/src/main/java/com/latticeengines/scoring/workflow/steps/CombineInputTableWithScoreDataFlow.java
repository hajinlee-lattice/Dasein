package com.latticeengines.scoring.workflow.steps;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CombineInputTableWithScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CombineInputTableWithScoreDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("combineInputTableWithScoreDataFlow")
public class CombineInputTableWithScoreDataFlow extends RunDataFlow<CombineInputTableWithScoreDataFlowConfiguration> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CombineInputTableWithScoreDataFlow.class);

    @Override
    public void execute() {
        setupDataFlow();
        super.execute();
        configureExport();
    }

    private void setupDataFlow() {
        CombineInputTableWithScoreParameters params = new CombineInputTableWithScoreParameters(
                getScoreResultTableName(), getInputTableName(), getBucketMetadata(), getModelType());
        setupCdlParameters(params);
        configuration.setDataFlowParams(params);
    }

    private void setupCdlParameters(CombineInputTableWithScoreParameters params) {
        if (!configuration.isCdlModel())
            return;
        String scoreFieldName = InterfaceName.Probability.name();
        if (configuration.isExpectedValue())
            scoreFieldName = InterfaceName.ExpectedRevenue.name();
        params.setScoreFieldName(scoreFieldName);
        Integer multiplier = null;
        if (!configuration.isExpectedValue() && !configuration.isLiftChart())
            multiplier = 100;
        params.setScoreMultiplier(multiplier);
        if (configuration.isLiftChart())
            params.setAvgScore(getDoubleValueFromContext(SCORING_AVG_SCORE));
        params.setIdColumn(InterfaceName.AnalyticPurchaseState_ID.toString());
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
        return configuration.getBucketMetadata();
    }

    private String getModelType() {
        return configuration.getModelType();
    }
}
