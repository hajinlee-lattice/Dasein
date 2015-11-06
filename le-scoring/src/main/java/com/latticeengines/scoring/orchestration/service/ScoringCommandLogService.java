package com.latticeengines.scoring.orchestration.service;

import java.util.List;

import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandLog;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;

public interface ScoringCommandLogService {

    public void log(ScoringCommand scoringCommand, String message);

    void logBeginStep(ScoringCommand scoringCommand, ScoringCommandStep step);

    void logCompleteStep(ScoringCommand scoringCommand, ScoringCommandStep step, ScoringCommandStatus status);

    void logLedpException(ScoringCommand scoringCommand, LedpException e);

    void logException(ScoringCommand scoringCommand, Exception e);

    void logYarnAppId(ScoringCommand scoringCommand, String yarnAppId, ScoringCommandStep step);

    List<ScoringCommandLog> findByScoringCommand(ScoringCommand scoringCommand);

    void logException(ScoringCommand scoringCommand, String message, Exception e);

}
