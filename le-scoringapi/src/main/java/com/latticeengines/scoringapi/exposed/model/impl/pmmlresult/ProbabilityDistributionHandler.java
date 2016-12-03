package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.EvaluationException;
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
        ProbabilityDistribution distribution = (ProbabilityDistribution) originalResult;
        Object r = null;
        
        try {
            r = distribution.getResult();
        } catch (EvaluationException e) {
            // this means it's Lattice RF model
        }
        
        double predicted = 0.0;
        
        if (r != null) {
            predicted = distribution.getProbability(r.toString());
        } else {
            predicted = distribution.getProbability("1");
        }

        result.put(ScoreType.PROBABILITY_OR_VALUE, null);
        result.put(ScoreType.PERCENTILE, new Double(predicted * 100).intValue());
    }

}
