package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.ProbabilityDistribution;
import org.springframework.stereotype.Component;

import com.latticeengines.scoringapi.exposed.ScoreType;

@Component
public class ProbabilityDistributionHandler extends PMMLResultHandlerBase {

    public ProbabilityDistributionHandler() {
        super(new Class<?>[] { ProbabilityDistribution.class });
    }

    @Override
    public void processResult(Map<ScoreType, Object> result, Object originalResult) {
        double predicted = ((ProbabilityDistribution) originalResult).getProbability("1");

        result.put(ScoreType.PROBABILITY_OR_VALUE, predicted);
        result.put(ScoreType.PERCENTILE, new Double(predicted * 100).intValue());
    }

}
