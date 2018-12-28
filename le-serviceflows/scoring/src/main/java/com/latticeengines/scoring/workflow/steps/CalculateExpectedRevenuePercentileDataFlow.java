package com.latticeengines.scoring.workflow.steps;

import java.util.Map;

import org.apache.commons.collections4.MapUtils;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.steps.CalculateExpectedRevenuePercentileDataFlowConfiguration;

@Component("calculateExpectedRevenuePercentileDataFlow")
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
public class CalculateExpectedRevenuePercentileDataFlow
        extends AbstractCalculateRevenuePercentileDataFlow<CalculateExpectedRevenuePercentileDataFlowConfiguration> {

    @Override
    String getRevenueFieldName() {
        return ScoreResultField.ExpectedRevenue.displayName;
    }

    @Override
    void initAndSetDataFlowParam(String inputTableName, String modelGuidField, int percentileLowerBound,
            int percentileUpperBound, Map<String, String> originalScoreFieldMap) {
        CalculateExpectedRevenuePercentileParameters params = new CalculateExpectedRevenuePercentileParameters();
        params.setInputTableName(inputTableName);
        params.setPercentileFieldName(ScoreResultField.ExpectedRevenuePercentile.displayName);
        params.setModelGuidField(modelGuidField);
        params.setPercentileLowerBound(percentileLowerBound);
        params.setPercentileUpperBound(percentileUpperBound);
        params.setCustomerSpace(configuration.getCustomerSpace());

        if (MapUtils.isNotEmpty(originalScoreFieldMap)) {
            params.setOriginalScoreFieldMap(originalScoreFieldMap);
        }
        configuration.setDataFlowParams(params);
    }
}
