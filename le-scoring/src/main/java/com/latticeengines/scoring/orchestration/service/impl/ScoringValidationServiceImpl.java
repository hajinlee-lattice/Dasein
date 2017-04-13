package com.latticeengines.scoring.orchestration.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.service.DbMetadataService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.scoring.orchestration.service.ScoringDaemonService;
import com.latticeengines.scoring.orchestration.service.ScoringValidationService;

@Component("scoringValidationService")
public class ScoringValidationServiceImpl implements ScoringValidationService {

    @Autowired
    private DbMetadataService dbMetadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Override
    public void validateBeforeProcessing(ScoringCommand scoringCommand) {
        validateTotal(scoringCommand);
        validateModelGuid(scoringCommand);
        validateLeadId(scoringCommand);
    }

    private void validateTotal(ScoringCommand scoringCommand) {
        long total = dbMetadataService.getRowCount(scoringJdbcTemplate, scoringCommand.getTableName());
        if (total != scoringCommand.getTotal()) {
            throw new LedpException(LedpCode.LEDP_20016);
        }
        if (total < 1) {
            throw new LedpException(LedpCode.LEDP_20017);
        }
    }

    private void validateModelGuid(ScoringCommand scoringCommand) {
        if (!dbMetadataService.checkIfColumnExists(scoringJdbcTemplate, scoringCommand.getTableName(),
                ScoringDaemonService.MODEL_GUID)) {
            throw new LedpException(LedpCode.LEDP_20004);
        }
    }

    private void validateLeadId(ScoringCommand scoringCommand) {
        if (!dbMetadataService.checkIfColumnExists(scoringJdbcTemplate, scoringCommand.getTableName(),
                ScoringDaemonService.UNIQUE_KEY_COLUMN)) {
            throw new LedpException(LedpCode.LEDP_20003);
        }
    }
}
