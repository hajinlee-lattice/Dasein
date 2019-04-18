package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.EvaluationException;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.VoteDistribution;
import org.springframework.stereotype.Component;

import com.latticeengines.scoringapi.exposed.ScoreType;

@Component
public class VoteDistributionHandler extends PMMLResultHandlerBase {

    public VoteDistributionHandler() {
        super(new Class<?>[] { VoteDistribution.class });
    }

    @Override
    public void processResult(Evaluator evaluator, Map<ScoreType, Object> result, Object originalResult) {
        VoteDistribution distribution = (VoteDistribution) originalResult;
        Object r = null;

        try {
            r = distribution.getResult();
            result.put(ScoreType.CLASSIFICATION, r);
        } catch (EvaluationException e) {
            // this means it's Lattice RF model
        }

        double predicted = distribution.getProbability((String) r);

        result.put(ScoreType.PROBABILITY_OR_VALUE, null);
        result.put(ScoreType.PERCENTILE, new Double(predicted * 100).intValue());
    }

}
