package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculatePercentileScoreParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.RecalculatePercentileScoreDataFlowConfiguration;
import com.latticeengines.serviceflows.workflow.dataflow.RunDataFlow;

@Component("recalculatePercentileScoreDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class RecalculatePercentileScoreDataFlow extends RunDataFlow<RecalculatePercentileScoreDataFlowConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(RecalculatePercentileScoreDataFlow.class);

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
        putStringValueInContext(SCORING_RESULT_TABLE_NAME, configuration.getTargetTableName());
    }

    private void preDataFlow() {
        String inputTableName = getStringValueFromContext(SCORING_RESULT_TABLE_NAME);

        RecalculatePercentileScoreParameters params = new RecalculatePercentileScoreParameters();
        params.setInputTableName(inputTableName);
        params.setRawScoreFieldName(rawScoreField);
        params.setScoreFieldName(scoreField);
        params.setModelGuidField(modelGuidField);
        params.setPercentileLowerBound(percentileLowerBound);
        params.setPercentileUpperBound(percentileUpperBound);

        Map<String, String> scoreFieldMap = ExpectedRevenueDataFlowUtil
                .getScoreFieldsMap(getListObjectFromContext(RATING_MODELS, RatingModelContainer.class));

        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            log.info(String.format("Using scoreFieldMap %s", JsonUtils.serialize(scoreFieldMap)));
            params.setOriginalScoreFieldMap(scoreFieldMap);
        } else {
            throw new RuntimeException("Couldn't find any valid scoreFieldMap or individual modelGuid");
        }

        configuration.setDataFlowParams(params);
    }
}
