package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.EvaluationException;
import org.jpmml.evaluator.Evaluator;
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
    public void processResult(Evaluator evaluator, Map<ScoreType, Object> result, Object originalResult) {
        ProbabilityDistribution distribution = (ProbabilityDistribution) originalResult;
        Object r = null;

        try {
            r = distribution.getResult();
            result.put(ScoreType.CLASSIFICATION, r);
        } catch (EvaluationException ignore) {
            // this means it's Lattice RF model
        }

        double predicted = 0.0;
        List<FieldName> outputFieldNames = evaluator.getOutputFields();
        if (outputFieldNames.size() == 3) {
            Set<String> outputValues = new HashSet<>();
            outputValues.add(evaluator.getOutputField(outputFieldNames.get(1)).getValue());
            outputValues.add(evaluator.getOutputField(outputFieldNames.get(2)).getValue());

            if (outputValues.equals(Sets.<String> newHashSet("0", "1"))) {
                predicted = distribution.getProbability("1");
                if (r == null) {
                    result.put(ScoreType.CLASSIFICATION, "1");
                }
                result.put(ScoreType.PROBABILITY_OR_VALUE, null);
                result.put(ScoreType.PERCENTILE, Double.valueOf(predicted * 100.).intValue());
                return;
            }
        }
        if (r != null) {
            predicted = distribution.getProbability(r.toString());
        } else {
            predicted = distribution.getProbability("1");
            result.put(ScoreType.CLASSIFICATION, "1");
        }
        result.put(ScoreType.PROBABILITY_OR_VALUE, null);
        result.put(ScoreType.PERCENTILE, Double.valueOf(predicted * 100.).intValue());
    }

}
