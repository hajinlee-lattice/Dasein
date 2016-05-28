package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.util.AbstractMap;

import org.springframework.http.HttpStatus;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.ExceptionHandlerErrors;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;
import com.latticeengines.testframework.rest.LedpResponseErrorHandler;

public class ScoringResourceErrorsDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    @Test(groups = "deployment", enabled = true)
    public void missingModelId() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId("");

        AbstractMap.SimpleEntry<LedpResponseErrorHandler, ExceptionHandlerErrors> handlerAndErrors = post(url,
                scoreRequest);
        Assert.assertEquals(handlerAndErrors.getKey().getStatusCode(), HttpStatus.BAD_REQUEST);
        Assert.assertEquals(handlerAndErrors.getValue().getError(), LedpCode.LEDP_31101.getExternalCode());
        Assert.assertEquals(handlerAndErrors.getValue().getDescription(), LedpCode.LEDP_31101.getMessage());
    }

    @Test(groups = "deployment", enabled = true)
    public void invalidModelId() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId("ScoringResourceTest_INVALID_MODEL_ID");

        AbstractMap.SimpleEntry<LedpResponseErrorHandler, ExceptionHandlerErrors> handlerAndErrors = post(url,
                scoreRequest);
        Assert.assertEquals(handlerAndErrors.getKey().getStatusCode(), HttpStatus.BAD_REQUEST);
        Assert.assertEquals(handlerAndErrors.getValue().getError(), LedpCode.LEDP_31102.getExternalCode());
    }

    @Test(groups = "deployment", enabled = true)
    public void missingDomain() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().put(InterfaceName.Email.name(), null);
        scoreRequest.getRecord().put(InterfaceName.CompanyName.name(), null);

        AbstractMap.SimpleEntry<LedpResponseErrorHandler, ExceptionHandlerErrors> handlerAndErrors = post(url,
                scoreRequest);
        Assert.assertEquals(handlerAndErrors.getKey().getStatusCode(), HttpStatus.BAD_REQUEST);
        Assert.assertEquals(handlerAndErrors.getValue().getError(), LedpCode.LEDP_31199.getExternalCode());
    }

    @Test(groups = "deployment", enabled = true)
    public void mismatchedDatatype() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().put("Activity_Count_Click_Email", "ModelExpects this to be a number");

        AbstractMap.SimpleEntry<LedpResponseErrorHandler, ExceptionHandlerErrors> handlerAndErrors = post(url, scoreRequest);
        Assert.assertEquals(handlerAndErrors.getKey().getStatusCode(), HttpStatus.BAD_REQUEST);
        Assert.assertEquals(handlerAndErrors.getValue().getError(), LedpCode.LEDP_31105.getExternalCode());
        Assert.assertTrue(handlerAndErrors.getValue().getDescription().contains("Activity_Count_Click_Email"));
    }

    private AbstractMap.SimpleEntry<LedpResponseErrorHandler, ExceptionHandlerErrors> post(String url,
            ScoreRequest scoreRequest) {
        LedpResponseErrorHandler ledpResponseErrorHandler = new LedpResponseErrorHandler();
        oAuth2RestTemplate.setErrorHandler(ledpResponseErrorHandler);

        try {
            oAuth2RestTemplate.postForEntity(url, scoreRequest, ExceptionHandlerErrors.class);
        } catch (Exception e) {
        }
        String responseText = ledpResponseErrorHandler.getResponseString();
        ExceptionHandlerErrors errors = JsonUtils.deserialize(responseText, ExceptionHandlerErrors.class);

        return new AbstractMap.SimpleEntry<>(ledpResponseErrorHandler, errors);
    }
}
