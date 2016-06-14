package com.latticeengines.scoringapi.exposed.model;

import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.scoringapi.exposed.ScoreType;

public interface ModelEvaluator {
    Map<ScoreType, Object> evaluate(Map<String, Object> record, ScoreDerivation derivation);
}
