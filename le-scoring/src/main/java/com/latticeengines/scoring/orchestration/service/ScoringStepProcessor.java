package com.latticeengines.scoring.orchestration.service;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;

public interface ScoringStepProcessor {

    void executeStep(ScoringCommand scoringCommand);

}
