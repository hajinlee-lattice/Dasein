package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.Classification;
import org.jpmml.evaluator.EvaluationException;
import org.jpmml.evaluator.Evaluator;
import org.jpmml.evaluator.NodeScoreDistribution;
import org.springframework.stereotype.Component;

import com.latticeengines.scoringapi.exposed.ScoreType;

@Component
public class ClassificationHandler extends PMMLResultHandlerBase {

    public ClassificationHandler() {
        super(new Class<?>[] { Classification.class, NodeScoreDistribution.class });
    }

    @Override
    public void processResult(Evaluator evaluator, Map<ScoreType, Object> result, Object originalResult) {
        Classification distribution = (Classification) originalResult;
        Object r = null;

        try {
            r = distribution.getResult();
            result.put(ScoreType.CLASSIFICATION, r);
        } catch (EvaluationException ignore) {
            // this means it's Lattice RF model
        }

        double predicted = 0.0;

        result.put(ScoreType.PROBABILITY_OR_VALUE, null);
        result.put(ScoreType.PERCENTILE, new Double(predicted * 100).intValue());
    }

}
