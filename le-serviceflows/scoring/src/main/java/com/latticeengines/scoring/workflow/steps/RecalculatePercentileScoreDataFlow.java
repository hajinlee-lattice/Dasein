package com.latticeengines.scoring.workflow.steps;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.RatingModelContainer;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.cdl.pa.ProcessAnalyzeWorkflowConfiguration;
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

    @Value("${cdl.big.dataflow.am.mem.gb}")
    private int amMemGbForPA;

    @Value("${cdl.big.dataflow.am.vcores}")
    private int amVCoresForPA;

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
        String modelGuid = getStringValueFromContext(SCORING_MODEL_ID);

        if (MapUtils.isNotEmpty(scoreFieldMap)) {
            log.info(String.format("Using scoreFieldMap %s", JsonUtils.serialize(scoreFieldMap)));
            params.setOriginalScoreFieldMap(scoreFieldMap);
        } else if (StringUtils.isNotBlank(modelGuid)) {
            log.info(String.format("Using individual modelGuid %s to set scoreFieldMap", modelGuid));
            scoreFieldMap = new HashMap<>();
            scoreFieldMap.put(modelGuid, ScoreResultField.RawScore.displayName);
            params.setOriginalScoreFieldMap(scoreFieldMap);
        } else {
            throw new RuntimeException("Couldn't find any valid scoreFieldMap or individual modelGuid");
        }

        configuration.setDataFlowParams(params);
    }

    private boolean inPA() {
        return getNamespace().contains(ProcessAnalyzeWorkflowConfiguration.WORKFLOW_NAME);
    }

    protected Integer getYarnAmMemGb() {
        if (inPA()) {
            return amMemGbForPA;
        } else {
            return super.getYarnAmMemGb();
        }
    }

    protected Integer getYarnAmVCores() {
        if (inPA()) {
            return amVCoresForPA;
        } else {
            return super.getYarnAmVCores();
        }
    }
}
