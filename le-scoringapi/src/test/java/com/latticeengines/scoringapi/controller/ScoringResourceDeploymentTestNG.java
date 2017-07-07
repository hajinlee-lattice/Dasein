package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.DateTimeUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.pls.BucketName;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugRecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

public class ScoringResourceDeploymentTestNG extends ScoringResourceDeploymentTestNGBase {

    private static final int SAFETY_RANGE = 1;

    @Test(groups = "deployment", enabled = true)
    public void getModels() {
        List<Model> models = getModelList();
        Assert.assertTrue(models.size() >= 1);
        Assert.assertEquals(models.get(0).getModelId(), MODEL_ID);
        Assert.assertEquals(models.get(0).getName(), MODEL_NAME);
    }

    private List<Model> getModelList() {
        String url = apiHostPort + "/score/models/CONTACT";
        ResponseEntity<List<Model>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Model>>() {
                });
        List<Model> models = response.getBody();
        return models;
    }

    @Test(groups = "deployment", enabled = true)
    public void getFields() {
        String modelId = MODEL_ID;
        Fields fields = getFieldsForModel(modelId);
        Assert.assertNotNull(fields);
        Assert.assertEquals(fields.getModelId(), modelId);

        for (Field field : fields.getFields()) {
            FieldSchema expectedSchema = eventTableDataComposition.fields.get(field.getFieldName());
            Assert.assertEquals(expectedSchema.type, field.getFieldType());
            Assert.assertEquals(expectedSchema.source, FieldSource.REQUEST);
        }
    }

    private Fields getFieldsForModel(String modelId) {
        String url = apiHostPort + "/score/models/" + modelId + "/fields";
        ResponseEntity<Fields> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<Fields>() {
                });
        Fields fields = response.getBody();
        return fields;
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreRecord() throws IOException {
        String url = apiHostPort + "/score/record";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<ScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                ScoreResponse.class);
        ScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreDebugRecord() throws IOException {
        String url = apiHostPort + "/score/record/debug";
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<DebugScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                DebugScoreResponse.class);

        DebugScoreResponse scoreResponse = response.getBody();
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        double difference = Math.abs(scoreResponse.getProbability() - 0.41640343016092707d);
        Assert.assertTrue(difference < 0.1);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreOutOfRangeRecord() throws IOException {
        String url = apiHostPort + "/score/record/debug";
        InputStream scoreRequestIs = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream(LOCAL_MODEL_PATH + "outofrange_score_request.json");
        String scoreRecordContents = IOUtils.toString(scoreRequestIs, Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);

        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<DebugScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                DebugScoreResponse.class);

        DebugScoreResponse scoreResponse = response.getBody();
        System.out.println(JsonUtils.serialize(scoreResponse));
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        Assert.assertTrue(scoreResponse.getProbability() > 0.27);
        Assert.assertNotNull(scoreResponse.getBucket());
        Assert.assertEquals(scoreResponse.getBucket(), BucketName.A.toValue());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelFieldsAfterScoring() {
        List<Model> models = getModelList();
        for (Model model : models) {
            Fields fields = getFieldsForModel(model.getModelId());
            checkFields(model.getName(), fields, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getPaginatedModels() throws ParseException {
        String url = apiHostPort + "/score";
        Long start = System.currentTimeMillis();
        List<ModelDetail> models = getPaginatedModels(url, new Date(0), true, 1, 50);
        System.out.println("Time taken in getPaginatedModels for " + models.size() + " models = "
                + (System.currentTimeMillis() - start) + " ms");
        checkModelDetails(models, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
    }

    @Test(groups = "deployment", enabled = true)
    public void getBaseModelsCount() {
        baseAllModelCount = getModelCount(1, true, new Date(), false);
        Assert.assertEquals(baseAllModelCount, 1);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountActive() {
        baseAllActiveModelCount = getModelCount(1, false, null, false);
        Assert.assertEquals(baseAllActiveModelCount, 1);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelsCountAfterBulkScoring() {
        getModelCount(baseAllModelCount + MAX_MODELS, true, null, true);
        getModelCount(0, false, new Date(), true);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords", "getModelsCountAfterBulkScoring",
            "getPaginatedModels" })
    public void getModelsCountAfterModelDelete() {
        TestRegisterModels modelCreator = new TestRegisterModels();
        modelCreator.deleteModel(plsRest, customerSpace, MODEL_ID);
        getModelCount(baseAllActiveModelCount + MAX_MODELS - 1, false, null, true);
        getModelCount(0, false, new Date(), true);
    }

    private int getModelCount(int n, boolean considerAllStatus, Date lastUpdateTime, boolean shouldAssert) {
        String url = apiHostPort + "/score/modeldetails/count?considerAllStatus=" + considerAllStatus;
        if (lastUpdateTime != null) {
            url += "&start=" + DateTimeUtils.convertToStringUTCISO8601(lastUpdateTime);
        }

        ResponseEntity<Integer> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null, Integer.class);
        int modelsCount = response.getBody();
        if (shouldAssert) {
            Assert.assertTrue(modelsCount >= n);
        }
        return n;
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "getBaseModelsCount", "getModelsCountActive" })
    public void scoreRecords() throws IOException, InterruptedException {
        final String url = apiHostPort + "/score/records";
        if (shouldRunScoringTest()) {
            runScoringTest(url);
        }
    }

    @SuppressWarnings("rawtypes")
    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void testScoreCorrectness() throws IOException {
        String url = apiHostPort + "/score/record/debug";
        List<ScoreRequest> scoreRequests = getScoreRequestsForScoreCorrectness();
        List<Integer> expectedScores = getExpectedScoresForScoreCorrectness();
        List<DebugScoreResponse> signleRecordScoreResponseList = new ArrayList<>();

        for (ScoreRequest scoreRequest : scoreRequests) {
            ResponseEntity<DebugScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                    DebugScoreResponse.class);
            signleRecordScoreResponseList.add(response.getBody());
        }

        url = apiHostPort + "/score/records/debug";
        BulkRecordScoreRequest bulkScoreRequest = getBulkScoreRequestForScoreCorrectness();

        List<?> resultObjList = null;
        ResponseEntity<List> response = oAuth2RestTemplate.postForEntity(url, bulkScoreRequest, List.class);
        resultObjList = response.getBody();

        int idx = 0;
        ObjectMapper om = new ObjectMapper();
        for (Object res : resultObjList) {
            System.out.println("Expected score = " + expectedScores.get(idx).intValue());
            DebugRecordScoreResponse result = om.readValue(om.writeValueAsString(res), DebugRecordScoreResponse.class);
            Assert.assertEquals(result.getScores().get(0).getScore().intValue(),
                    new Double(signleRecordScoreResponseList.get(idx).getScore()).intValue());
            assertScoreIsWithinAcceptableRange(result.getScores().get(0).getScore().intValue(),
                    expectedScores.get(idx).intValue());
            matchTransformedRecord(signleRecordScoreResponseList.get(idx).getTransformedRecord(),
                    result.getTransformedRecordMap().get(result.getScores().get(0).getModelId()));
            idx++;
        }
    }

    private void assertScoreIsWithinAcceptableRange(int score, int expectedScore) {
        Assert.assertTrue(Math.abs(score - expectedScore) <= SAFETY_RANGE);
    }

    private void matchTransformedRecord(Map<String, Object> singleRecordScoreTransformedRecord,
            Map<String, Object> batchScoreTransformedRecord) {

        if (singleRecordScoreTransformedRecord.size() != batchScoreTransformedRecord.size()) {
            for (String key : singleRecordScoreTransformedRecord.keySet()) {
                if (!batchScoreTransformedRecord.containsKey(key)) {
                    System.out.println("Extra key present in singleRecordScoreTransformedRecord: " + key);
                }
            }

            try {
                ObjectMapper om = new ObjectMapper();
                for (String key : singleRecordScoreTransformedRecord.keySet()) {
                    if (batchScoreTransformedRecord.containsKey(key)) {
                        if (singleRecordScoreTransformedRecord.get(key) != null
                                && batchScoreTransformedRecord.get(key) != null) {
                            if (!om.writeValueAsString(singleRecordScoreTransformedRecord.get(key))
                                    .equals(om.writeValueAsString(batchScoreTransformedRecord.get(key)))) {
                                System.out.println("Value mismatch singleRecordScoreTransformedRecord: "
                                        + om.writeValueAsString(singleRecordScoreTransformedRecord.get(key))
                                        + " and batchScoreTransformedRecord: "
                                        + om.writeValueAsString(batchScoreTransformedRecord.get(key)));
                            }
                        } else {
                            if (singleRecordScoreTransformedRecord.get(key) != null) {
                                System.out.println(
                                        "Value mismatch batchScoreTransformedRecord: null and singleRecordScoreTransformedRecord: "
                                                + om.writeValueAsString(singleRecordScoreTransformedRecord.get(key)));
                            } else if (batchScoreTransformedRecord.get(key) != null) {
                                System.out.println(
                                        "Value mismatch singleRecordScoreTransformedRecord: null and batchScoreTransformedRecord: "
                                                + om.writeValueAsString(batchScoreTransformedRecord.get(key)));
                            }
                        }
                    }
                }
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        }

        Assert.assertEquals(singleRecordScoreTransformedRecord.size(), batchScoreTransformedRecord.size());
        Assert.assertTrue(singleRecordScoreTransformedRecord.size() > 0);

        for (String key : singleRecordScoreTransformedRecord.keySet()) {
            Assert.assertTrue(batchScoreTransformedRecord.containsKey(key));
            Assert.assertEquals(singleRecordScoreTransformedRecord.get(key), batchScoreTransformedRecord.get(key));
        }
    }

    protected boolean shouldRunScoringTest() {
        return true;
    }

    @Override
    protected boolean shouldUseAppId() {
        return true;
    }

    @Override
    protected String getAppIdForOauth2() {
        return "DUMMY_APP2";
    }

    @Override
    protected boolean shouldSelectAttributeBeforeTest() {
        return false;
    }

    private List<ModelDetail> getPaginatedModels(String serviceHostPort, Date start, boolean considerAllStatus,
            int offset, int maximum) {
        String url = serviceHostPort
                + "/modeldetails?considerAllStatus={considerAllStatus}&offset={offset}&maximum={maximum}&start={start}";
        String startStr = DateTimeUtils.convertToStringUTCISO8601(start);
        System.out.println(url);
        ResponseEntity<List<ModelDetail>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<ModelDetail>>() {
                }, considerAllStatus, offset, maximum, startStr);
        return response.getBody();
    }
}
