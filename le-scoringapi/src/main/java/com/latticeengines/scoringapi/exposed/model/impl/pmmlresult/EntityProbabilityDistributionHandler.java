package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.EntityProbabilityDistribution;
import org.jpmml.evaluator.Evaluator;
import org.springframework.stereotype.Component;

import com.latticeengines.scoringapi.exposed.ScoreType;

@Component
public class EntityProbabilityDistributionHandler extends PMMLResultHandlerBase {

    public EntityProbabilityDistributionHandler() {
        super(new Class<?>[] { EntityProbabilityDistribution.class });
    }

    @Override
    public void processResult(Evaluator evaluator, Map<ScoreType, Object> result, Object originalResult) {
        EntityProbabilityDistribution<?> distribution = (EntityProbabilityDistribution<?>) originalResult;
        Object r = distribution.getResult();
        result.put(ScoreType.CLASSIFICATION, r);

        double predicted = distribution.getProbability(r.toString());
        result.put(ScoreType.PROBABILITY_OR_VALUE, null);
        result.put(ScoreType.PERCENTILE, Double.valueOf(predicted * 100.).intValue());

    }

}
