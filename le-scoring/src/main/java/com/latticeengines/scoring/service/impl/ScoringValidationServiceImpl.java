package com.latticeengines.scoring.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.dataplatform.exposed.service.MetadataService;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.scoring.ScoringCommand;
import com.latticeengines.scoring.service.ScoringDaemonService;
import com.latticeengines.scoring.service.ScoringValidationService;

@Component("scoringValidationService")
public class ScoringValidationServiceImpl implements ScoringValidationService {

    @Autowired
    private MetadataService metadataService;

    @Autowired
    private JdbcTemplate scoringJdbcTemplate;

    @Override
    public void validateBeforeProcessing(ScoringCommand scoringCommand) {
        validateTotal(scoringCommand);
        validateModelGUID(scoringCommand);
    }

    private void validateTotal(ScoringCommand scoringCommand) {
        long total = metadataService.getRowCount(scoringJdbcTemplate, scoringCommand.getTableName());
        if (total != scoringCommand.getTotal()) {
            throw new LedpException(LedpCode.LEDP_20016);
        }
        if (total < 1) {
            throw new LedpException(LedpCode.LEDP_20017);
        }
    }

    private void validateModelGUID(ScoringCommand scoringCommand) {
        if (!metadataService.checkIfColumnExists(scoringJdbcTemplate, scoringCommand.getTableName(),
                ScoringDaemonService.MODEL_GUID)) {
            throw new LedpException(LedpCode.LEDP_20004);
        }
    }
}
