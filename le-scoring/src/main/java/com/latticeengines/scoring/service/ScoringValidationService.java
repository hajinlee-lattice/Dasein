package com.latticeengines.scoring.service;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;

public interface ScoringValidationService {

    void validateBeforeProcessing(ScoringCommand scoringCommand);

}
