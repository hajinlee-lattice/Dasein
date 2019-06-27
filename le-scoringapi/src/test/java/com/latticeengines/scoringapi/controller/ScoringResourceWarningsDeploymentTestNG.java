package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class ScoringResourceWarningsDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(ScoringResourceWarningsDeploymentTestNG.class);

    @Test(groups = "deployment", enabled = true)
    public void missingColumn() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().remove("kickboxDisposable");
        scoreRequest.getRecord().remove("HasEEDownload");
        List<String> values = new ArrayList<>();
        values.add("kickboxDisposable");
        values.add("HasEEDownload");

        Map<String, List<String>> expectedWarningCodeAndMessageValues = new HashMap<>();
        expectedWarningCodeAndMessageValues.put(WarningCode.MISSING_COLUMN.getExternalCode(), values);

        postAndAssert(url, scoreRequest, expectedWarningCodeAndMessageValues);
    }

    @Test(groups = "deployment", enabled = true)
    public void noMatch() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().put("Email", "johndoe@abcdwrwr");
        scoreRequest.getRecord().put("CompanyName", "abcdwrwr");
        List<String> values = new ArrayList<>();
        values.add("abcdwrwr");

        Map<String, List<String>> expectedWarningCodeAndMessageValues = new HashMap<>();
        // expectedWarningCodeAndMessageValues.put(WarningCode.NO_MATCH.getExternalCode(), values);

        postAndAssert(url, scoreRequest, expectedWarningCodeAndMessageValues);
    }

    @Test(groups = "deployment", enabled = true)
    public void publicDomain() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().put("Email", "johndoe@gmail.com");
        List<String> values = new ArrayList<>();
        values.add("gmail.com");

        Map<String, List<String>> expectedWarningCodeAndMessageValues = new HashMap<>();
        // expectedWarningCodeAndMessageValues.put(WarningCode.PUBLIC_DOMAIN.getExternalCode(), values);

        postAndAssert(url, scoreRequest, expectedWarningCodeAndMessageValues);
    }

    @Test(groups = "deployment", enabled = true)
    public void extraFields() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().put("ScoringTest_ExtraFieldA", "1A");
        scoreRequest.getRecord().put("ScoringTest_ExtraFieldB", "1B");
        scoreRequest.getRecord().put("ScoringTest_ExtraFieldC", "1C");

        List<String> values = new ArrayList<>();
        values.add("ScoringTest_ExtraFieldA");
        values.add("ScoringTest_ExtraFieldB");
        values.add("ScoringTest_ExtraFieldC");

        Map<String, List<String>> expectedWarningCodeAndMessageValues = new HashMap<>();
        expectedWarningCodeAndMessageValues.put(WarningCode.EXTRA_FIELDS.getExternalCode(), values);

        postAndAssert(url, scoreRequest, expectedWarningCodeAndMessageValues);
    }

    @Test(groups = "deployment", enabled = true)
    public void mismatchedDatatype() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.getRecord().put("Activity_Count_Click_Email",
                "$200 to $1000 range ModelExpects this to be a number");
        List<String> values = new ArrayList<>();
        values.add("Activity_Count_Click_Email");
        values.add("$200 to $1000 range ModelExpects this to be a number");
        values.add(scoreRequest.getModelId());

        Map<String, List<String>> expectedWarningCodeAndMessageValues = new HashMap<>();
        expectedWarningCodeAndMessageValues.put(WarningCode.MISMATCHED_DATATYPE.getExternalCode(), values);
    }

    private void postAndAssert(String url, ScoreRequest scoreRequest,
            Map<String, List<String>> expectedWarningCodeAndMessageValues) {
        ResponseEntity<ScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                ScoreResponse.class);
        log.info(JsonUtils.serialize(response));

        Map<String, String> observedWarningCodes = new HashMap<>();
        ScoreResponse scoreResponse = response.getBody();
        List<Warning> warnings = scoreResponse.getWarnings();
        for (Warning warning : warnings) {
            observedWarningCodes.put(warning.getWarning(), warning.getDescription());
        }
        // Assert.assertTrue(
        // SetUtils.isEqualSet(observedWarningCodes.keySet(), expectedWarningCodeAndMessageValues.keySet()),
        // String.format("oberved Keys=%s, expectedKeys=%s", observedWarningCodes.keySet(),
        // expectedWarningCodeAndMessageValues.keySet()));
        // for (String warningCode : expectedWarningCodeAndMessageValues.keySet()) {
        // String observedDescription = observedWarningCodes.get(warningCode);
        // for (String warningValue : expectedWarningCodeAndMessageValues.get(warningCode)) {
        // Assert.assertTrue(observedDescription.toUpperCase().contains(warningValue.toUpperCase()), "*****Expecting
        // warning:::" + warningValue.toUpperCase());
        // }
        // }
        // TODO - removed after replacing data models
    }
}
