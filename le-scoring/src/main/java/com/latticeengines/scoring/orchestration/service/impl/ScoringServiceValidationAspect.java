package com.latticeengines.scoring.orchestration.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.domain.exposed.scoring.ScoringCommandResult;
import com.latticeengines.domain.exposed.scoring.ScoringCommandState;
import com.latticeengines.domain.exposed.scoring.ScoringCommandStep;
import com.latticeengines.scoring.entitymanager.ScoringCommandResultEntityMgr;
import com.latticeengines.scoring.entitymanager.ScoringCommandStateEntityMgr;
import com.latticeengines.scoring.orchestration.service.ScoringCommandLogService;

@Component("scoringServiceValidationAspect")
public class ScoringServiceValidationAspect {

    @Autowired
    private ScoringCommandLogService scoringCommandLogService;

    @Autowired
    private ScoringCommandResultEntityMgr scoringCommandResultEntityMgr;

    @Autowired
    private ScoringCommandStateEntityMgr scoringCommandStateEntityMgr;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Autowired
    private MetadataService metadataService;

    public void validateScoreResult(ScoringCommand scoringCommand){
        ScoringCommandState state = scoringCommandStateEntityMgr.findByScoringCommandAndStep(scoringCommand, ScoringCommandStep.EXPORT_DATA);
        ScoringCommandResult result = scoringCommandResultEntityMgr.findByKey(state.getLeadOutputQueuePid());
        scoringCommandLogService.log(scoringCommand, "Populated: " + metadataService.getRowCount(scoringJdbcTemplate, result.getTableName()) + " rows of score result");
    }
}
