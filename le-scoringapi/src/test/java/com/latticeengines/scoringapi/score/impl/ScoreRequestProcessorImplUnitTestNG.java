package com.latticeengines.scoringapi.score.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.scoringapi.exposed.exception.ScoringApiException;

public class ScoreRequestProcessorImplUnitTestNG {

    private static final Logger log = LoggerFactory.getLogger(ScoreRequestProcessorImplUnitTestNG.class);
    private ScoreRequestProcessorImpl scoreProcessor = new ScoreRequestProcessorImpl();
    private String modelId = "ms__cd1fa727-b32b-47bb-b622-8455ef2e1ff7-PLSModel";

    @Test(groups = "unit")
    public void testLedpCode() {
        LedpCode code = LedpCode.LEDP_31026;
        log.info("code.getExternalCode() = " + code.getExternalCode());
    }

    @Test(groups = "unit")
    public void testHandleLedpException() {
        LedpException e = new LedpException(LedpCode.LEDP_31026, new String[] { modelId });
        ScoringApiException scoringApiException = scoreProcessor.handleLedpException(e);
        log.info("errorCode = " + scoringApiException.getCode().name());
        log.info("errorDescription = " + scoringApiException.getMessage());
    }
}
