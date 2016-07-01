package com.latticeengines.scoringapi.controller;

import java.io.IOException;

import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

public class ScoringResourceWithAppIDDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {

    @Test(groups = "deployment", enabled = true)
    public void scoreRecord() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<ScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                ScoreResponse.class);
        ScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
    }

    @Override
    protected boolean shouldUseAppId() {
        return true;
    }

    @Override
    protected String getAppIdForOauth2() {
        return "DUMMY_APP";
    }
}
