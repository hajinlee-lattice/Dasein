package com.latticeengines.scoringapi.exposed.model.impl.pmmlresult;

import java.util.Map;

import com.latticeengines.scoringapi.exposed.ScoreType;

public interface PMMLResultHandler {

    void processResult(Map<ScoreType, Object> result, Object originalResult);
}
