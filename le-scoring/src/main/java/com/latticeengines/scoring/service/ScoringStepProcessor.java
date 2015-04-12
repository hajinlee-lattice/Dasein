package com.latticeengines.scoring.service;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;

public interface ScoringStepProcessor {

    void executeStep(ScoringCommand scoringCommand);

}
