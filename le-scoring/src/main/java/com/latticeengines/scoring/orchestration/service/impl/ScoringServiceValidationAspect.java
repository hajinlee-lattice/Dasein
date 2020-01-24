package com.latticeengines.scoring.orchestration.service.impl;

import javax.inject.Inject;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringCommandLogService;

@Component("scoringServiceValidationAspect")
public class ScoringServiceValidationAspect {

    @Inject
    private ScoringCommandLogService scoringCommandLogService;

    @Inject
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Inject
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Inject
    private JdbcTemplate scoringJdbcTemplate;

    @Inject
    private DbMetadataService dbMetadataService;

    public void validateScoreResult(ScoringCommand scoringCommand){
        ScoringCommandState state = scoringCommandStateEntityMgr.findByScoringCommandAndStep(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        ScoringCommandResult result = scoringCommandResultEntityMgr.findByKey(state.getLeadOutputQueuePid());
        scoringCommandLogService.log(scoringCommand, "Populated: " + dbMetadataService.getRowCount(scoringJdbcTemplate, result.getTableName()) + " rows of score result");
    }
}
