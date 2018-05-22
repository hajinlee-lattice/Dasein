package com.latticeengines.scoring.workflow.steps;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.GenerateTargetScorePercentileMapParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculatePercentileScoreDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("recalculatePercentileScoreDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculatePercentileScoreDataFlow extends RunDataFlow<RecalculatePercentileScoreDataFlowConfiguration> {

    private static final String rawScoreField = ScoreResultField.RawScore.displayName;

    private static final String scoreField = ScoreResultField.Percentile.displayName;

    private static final String modelGuidField = ScoreResultField.ModelId.displayName;

    private static final int percentileLowerBound = 5;

    private static final int percentileUpperBound = 99;

    @Override
    public void execute() {
        preDataFlow();
        super.execute();
    }

    @Override
    public void onExecutionCompleted() {
        // todo, set values in context depending on where we are in the workflow
        // putStringValueInContext(SCORING_RESULT_TABLE_NAME), configuration.getTargetTableName());
    }

    private void preDataFlow() {
        String inputTableName = getStringValueFromContext(MAP_TARGET_SCORE_INPUT_TABLE_NAME);

        GenerateTargetScorePercentileMapParameters params = new GenerateTargetScorePercentileMapParameters();
        params.setInputTableName(inputTableName);
        params.setRawScoreFieldName(rawScoreField);
        params.setScoreFieldName(scoreField);
        params.setModelGuidField(modelGuidField);
        params.setPercentileLowerBound(percentileLowerBound);
        params.setPercentileUpperBound(percentileUpperBound);
        configuration.setDataFlowParams(params);
    }
}
