package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.DebugScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class ScoringResourceDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final String SALESFORCE = "SALESFORCE";
    private static final int MAX_FOLD_FOR_TIME_TAKEN = 7;
    // allow atleast 30 seconds of upper bound for bulk scoring api to make sure
    // that this testcase can work if performance is fine. If performance
    // degrades a lot in future then this limit will correctly fail the testcase
    private static final long MIN_UPPER_BOUND = TimeUnit.SECONDS.toMillis(30);
    private static final double EXPECTED_SCORE_99 = 99.0d;
    private static final int MAX_THREADS = 2;
    private volatile Throwable exception = null;

    @Test(groups = "deployment", enabled = true)
    public void getModels() {
        String url = apiHostPort + "/score/models/CONTACT";
        ResponseEntity<List<Model>> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<List<Model>>() {
                });
        List<Model> models = response.getBody();
        Assert.assertEquals(models.size(), 1);
        Assert.assertEquals(models.get(0).getModelId(), MODEL_ID);
        Assert.assertEquals(models.get(0).getName(), MODEL_NAME);
    }

    @Test(groups = "deployment", enabled = true)
    public void getFields() {
        String modelId = MODEL_ID;
        String url = apiHostPort + "/score/models/" + modelId + "/fields";
        ResponseEntity<Fields> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null,
                new ParameterizedTypeReference<Fields>() {
                });
        Fields fields = response.getBody();
        Assert.assertNotNull(fields);
        Assert.assertEquals(fields.getModelId(), modelId);

        for (Field field : fields.getFields()) {
            FieldSchema expectedSchema = eventTableDataComposition.fields.get(field.getFieldName());
            Assert.assertEquals(expectedSchema.type, field.getFieldType());
            Assert.assertEquals(expectedSchema.source, FieldSource.REQUEST);
        }
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
        double difference = Math.abs(scoreResponse.getProbability() - 0.5411256857185404d);
        Assert.assertTrue(difference < 0.1);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreOutOfRangeRecord() throws IOException {
        String url = apiHostPort + "/score/record/debug";
        URL scoreRequestUrl = ClassLoader.getSystemResource(LOCAL_MODEL_PATH + "outofrange_score_request.json");
        String scoreRecordContents = Files.toString(new File(scoreRequestUrl.getFile()), Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);

        scoreRequest.setModelId(MODEL_ID);
        ResponseEntity<DebugScoreResponse> response = oAuth2RestTemplate.postForEntity(url, scoreRequest,
                DebugScoreResponse.class);

        DebugScoreResponse scoreResponse = response.getBody();
        System.out.println(JsonUtils.serialize(scoreResponse));
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        Assert.assertTrue(scoreResponse.getProbability() > 0.27);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreRecords() throws IOException, InterruptedException {
        final String url = apiHostPort + "/score/records";

        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                try {
                    long timeTaken = 0;
                    long baselineTimeForOneEntry = testScore(url, 1, timeTaken);

                    long upperBoundForBulkScoring = baselineTimeForOneEntry * MAX_FOLD_FOR_TIME_TAKEN;
                    if (upperBoundForBulkScoring < MIN_UPPER_BOUND) {
                        upperBoundForBulkScoring = MIN_UPPER_BOUND;
                    }

                    System.out.println(upperBoundForBulkScoring);
                    testScore(url, 4, upperBoundForBulkScoring);
                    testScore(url, 8, upperBoundForBulkScoring);
                    testScore(url, 12, upperBoundForBulkScoring);
                    testScore(url, 16, upperBoundForBulkScoring);
                    testScore(url, 20, upperBoundForBulkScoring);
                    testScore(url, 40, upperBoundForBulkScoring);
                    testScore(url, 100, upperBoundForBulkScoring);
                    testScore(url, 200, upperBoundForBulkScoring);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < MAX_THREADS; i++) {
            Thread th = new Thread(runnable);
            th.start();
            threads.add(th);
        }

        for (Thread th : threads) {
            th.join();
        }

        if (exception != null) {
            Assert.assertNull(exception, "Got exception in one of the thread: " + getExceptionTrace(exception));
        }
    }

    private String getExceptionTrace(Throwable exception2) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        exception.printStackTrace(printWriter);
        printWriter.flush();

        String stackTrace = writer.toString();
        return stackTrace.toString();
    }

    private long testScore(String url, int n, long maxTime) throws IOException {
        try {
            BulkRecordScoreRequest bulkScoreRequest = getBulkScoreRequest(n);
            ObjectMapper om = new ObjectMapper();
            long timeDuration = System.currentTimeMillis();
            ResponseEntity<List> response = oAuth2RestTemplate.postForEntity(url, bulkScoreRequest, List.class);
            timeDuration = System.currentTimeMillis() - timeDuration;
            Assert.assertEquals(response.getBody().size(), n);

            int idx = 0;
            for (Object res : response.getBody()) {
                RecordScoreResponse result = om.readValue(om.writeValueAsString(res), RecordScoreResponse.class);
                if (idx % 2 == 1) {
                    Assert.assertEquals(result.getScores().size(), 2);
                    Assert.assertEquals(result.getScores().get(1).getScore(), EXPECTED_SCORE_99);
                    Assert.assertEquals(result.getScores().get(1).getModelId(), MODEL_ID);
                } else {
                    Assert.assertEquals(result.getScores().size(), 1);
                }
                Assert.assertEquals(result.getScores().get(0).getScore(), EXPECTED_SCORE_99);
                Assert.assertEquals(result.getScores().get(0).getModelId(), MODEL_ID);

                Record record = bulkScoreRequest.getRecords().get(idx++);
                if (Record.LATTICE_ID.equals(record.getIdType())) {
                    Assert.assertEquals(result.getLatticeId(), record.getRecordId());
                } else {
                    Assert.assertNotEquals(result.getLatticeId(), record.getRecordId());
                }

                Assert.assertEquals(result.getId(), record.getRecordId());

                if (record.isPerformEnrichment()) {
                    Assert.assertNotNull(result.getEnrichmentAttributeValues());
                    Assert.assertTrue(result.getEnrichmentAttributeValues().size() > 0);
                } else {
                    Assert.assertNull(result.getEnrichmentAttributeValues());
                }

            }

            if (maxTime > 0) {
                Assert.assertTrue(maxTime >= timeDuration,
                        "Time taken " + timeDuration + " for " + n + " records should be less than " + maxTime);
            }
            return timeDuration;
        } catch (Throwable ex) {
            exception = ex;
            throw ex;
        }
    }

    private BulkRecordScoreRequest getBulkScoreRequest(int n) throws IOException {
        BulkRecordScoreRequest bulkRequest = new BulkRecordScoreRequest();
        bulkRequest.setRule("Dummy Rule");
        bulkRequest.setSource("Dummy Source");

        List<Record> records = generateRecords(n);

        bulkRequest.setRecords(records);

        return bulkRequest;
    }

    private List<Record> generateRecords(int n) throws IOException {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Record record = new Record();
            record.setRecordId(UUID.randomUUID().toString());
            ScoreRequest scoreRequest = getScoreRequest();
            Map<String, Object> attributeValues = scoreRequest.getRecord();
            record.setAttributeValues(attributeValues);
            List<String> modelIds = new ArrayList<>();
            modelIds.add(MODEL_ID);
            record.setIdType(SALESFORCE);
            if (i % 2 == 1) {
                record.setIdType(Record.LATTICE_ID);
                record.setPerformEnrichment(true);
                modelIds.add(MODEL_ID);
            }
            record.setModelIds(modelIds);
            records.add(record);
        }
        return records;
    }
}
