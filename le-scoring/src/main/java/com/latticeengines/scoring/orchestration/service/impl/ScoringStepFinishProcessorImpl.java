package com.latticeengines.scoring.orchestration.service.impl;


import java.sql.Timestamp;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStatus;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringStepProcessor;

@Component("scoringStepFinishProcessor")
public class ScoringStepFinishProcessorImpl implements ScoringStepProcessor {

    @Autowired
    private ScoringCommandEntityMgr scoringCommandEntityMgr;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Override 
    public void executeStep(ScoringCommand scoringCommand) {
        DateTime dt = new DateTime(DateTimeZone.UTC);
        ScoringCommandState state = scoringCommandStateEntityMgr.findByScoringCommandAndStep(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        ScoringCommandResult result = scoringCommandResultEntityMgr.findByKey(state.getLeadOutputQueuePid());
        result.setStatus(ScoringCommandStatus.POPULATED);
        result.setPopulated(new Timestamp(dt.getMillis()));
        result.setTotal(scoringCommand.getTotal());
        scoringCommandResultEntityMgr.update(result);

        scoringCommand.setConsumed(new Timestamp(dt.getMillis()));
        scoringCommand.setStatus(ScoringCommandStatus.CONSUMED);
        scoringCommandEntityMgr.update(scoringCommand);
    }
}
