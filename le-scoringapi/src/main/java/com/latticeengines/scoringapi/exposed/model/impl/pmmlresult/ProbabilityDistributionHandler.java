package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.EvaluationException;
import org.jpmml.evaluator.ProbabilityDistribution;
import org.springframework.stereotype.Component;

import com.google.common.collect.Sets;
import com.latticeengines.scoringapi.exposed.ScoreType;

@Component
public class ProbabilityDistributionHandler extends PMMLResultHandlerBase {

    public ProbabilityDistributionHandler() {
        super(new Class<?>[] { ProbabilityDistribution.class });
    }

    @Override
    public void processResult(Map<ScoreType, Object> result, Object originalResult) {
        ProbabilityDistribution distribution = (ProbabilityDistribution) originalResult;
        Object r = null;

        try {
            r = distribution.getResult();
            result.put(ScoreType.CLASSIFICATION, r);
        } catch (EvaluationException e) {
            // this means it's Lattice RF model
        }

        double predicted = 0.0;
        if (distribution.getCategoryValues().size() == 2
                && distribution.getCategoryValues().equals(Sets.newHashSet("0", "1"))) {
            predicted = distribution.getProbability("1");
            if (r == null) {
                result.put(ScoreType.CLASSIFICATION, "1");
            }
        } else {
            if (r != null) {
                predicted = distribution.getProbability(r.toString());
            } else {
                predicted = distribution.getProbability("1");
                result.put(ScoreType.CLASSIFICATION, "1");
            }
        }

        result.put(ScoreType.PROBABILITY_OR_VALUE, null);
        result.put(ScoreType.PERCENTILE, new Double(predicted * 100).intValue());
    }

}
