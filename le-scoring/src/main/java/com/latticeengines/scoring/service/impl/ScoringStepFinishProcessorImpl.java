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
        //TODO Create a new table
        ScoringCommandResult result = new ScoringCommandResult(scoringCommand.getId(), ScoringCommandStatus.POPULATED,
                "TABLE", scoringCommand.getTotal(), new Timestamp(System.currentTimeMillis()));
        scoringCommandResultEntityMgr.create(result);

        scoringCommand.setConsumed(new Timestamp(System.currentTimeMillis()));
        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandEntityMgr.update(scoringCommand);
    }
}
