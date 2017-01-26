package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import org.jpmml.evaluator.Evaluator;

import com.latticeengines.scoringapi.exposed.ScoreType;

public interface PMMLResultHandler {

    void processResult(Evaluator evaluator, Map<ScoreType, Object> result, Object originalResult);
}
