package com.latticeengines.scoring.service.impl;

import java.sql.Timestamp;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.service.ScoringStepProcessor;

@Component("scoringStepFinishProcessor")
public class ScoringStepFinishProcessorImpl implements ScoringStepProcessor {

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Override 
    public void executeStep(ScoringCommand scoringCommand) {
        ScoringCommandResult result = scoringCommandResultEntityMgr.findByScoringCommand(scoringCommand);
        result.setStatus(ScoringCommandStatus.POPULATED);
        result.setPopulated(new Timestamp(System.currentTimeMillis()));
        result.setTotal(scoringCommand.getTotal());
        scoringCommandResultEntityMgr.update(result);

        scoringCommand.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandEntityMgr.update(scoringCommand);
    }
}
