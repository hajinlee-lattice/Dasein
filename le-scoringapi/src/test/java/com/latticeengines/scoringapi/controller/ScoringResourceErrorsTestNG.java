package com.latticeengines.scoringapi.controller;

import java.io.IOException;

import org.springframework.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.ExceptionHandlerErrors;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerTestNGBase;
import com.latticeengines.testframework.rest.LedpResponseErrorHandler;

public class ScoringResourceErrorsTestNG extends ScoringApiControllerTestNGBase {

    @Test(groups = "functional", enabled = true)
    public void missingModelId() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId("");

        LedpResponseErrorHandler ledpResponseErrorHandler = new LedpResponseErrorHandler();
        oAuth2RestTemplate.setErrorHandler(ledpResponseErrorHandler);

        try {
            oAuth2RestTemplate.postForEntity(url, scoreRequest, ExceptionHandlerErrors.class);
        } catch (Exception e) {
        }
        String responseText = ledpResponseErrorHandler.getResponseString();
        ExceptionHandlerErrors errors = JsonUtils.deserialize(responseText, ExceptionHandlerErrors.class);
        Assert.assertEquals(ledpResponseErrorHandler.getStatusCode(), HttpStatus.BAD_REQUEST);
        Assert.assertEquals(errors.getError(), LedpCode.LEDP_31101.getExternalCode());
        Assert.assertEquals(errors.getDescription(), LedpCode.LEDP_31101.getMessage());
    }

}
