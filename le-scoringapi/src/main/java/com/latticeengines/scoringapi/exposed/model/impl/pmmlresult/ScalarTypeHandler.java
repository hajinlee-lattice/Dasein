package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.scoringapi.exposed.ScoreType;

@Component
public class ScalarTypeHandler extends PMMLResultHandlerBase {

    public ScalarTypeHandler() {
        super(new Class<?>[] { Integer.class, Float.class, Double.class });
    }

    @Override
    public void processResult(Map<ScoreType, Object> result, Object originalResult) {
        double predicted = Double.valueOf(originalResult.toString());

        result.put(ScoreType.PROBABILITY_OR_VALUE, predicted);
        result.put(ScoreType.PERCENTILE, -1);
    }

}
