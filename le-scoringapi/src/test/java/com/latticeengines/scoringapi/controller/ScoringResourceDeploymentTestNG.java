package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
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
    private static final String MISSING_FIELD_COUNTRY = "Country";
    private static final String MISSING_FIELD_FIRSTNAME = "FirstName";
    private static final int MAX_FOLD_FOR_TIME_TAKEN = 10;
    // allow atleast 80 seconds of upper bound for bulk scoring api to make sure
    // that this testcase can work if performance is fine. If performance
    // degrades a lot in future then this limit will correctly fail the testcase
    private static final long MIN_UPPER_BOUND = TimeUnit.SECONDS.toMillis(80);
    private static final long MAX_UPPER_BOUND = TimeUnit.SECONDS.toMillis(120);
    private static final double EXPECTED_SCORE_99 = 99.0d;
    private static final int MAX_THREADS = 1;
    private static final int RECORD_MODEL_CARDINALITY = 2;
    private static final int MAX_MODELS = 20;
    private volatile Throwable exception = null;
    private static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";
    private static final String UTC = "UTC";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);
    private Map<String, List<String>> threadPerfMap = new HashMap<>();
    private boolean shouldPrintPerformanceInfo = true;

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

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
    public void getModelsCountAll() {
        getModelCount(1, true, null);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountActive() {
        getModelCount(1, false, null);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelsCountAfterBulkScoring() {
        getModelCount(1 + MAX_MODELS, true, null);
        getModelCount(0, false, new Date());
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords",
            "getModelsCountAfterBulkScoring" })
    public void getModelsCountAfterModelDelete() {
        TestRegisterModels modelCreator = new TestRegisterModels();
        modelCreator.deleteModel(plsRest, customerSpace, MODEL_ID);
        getModelCount(MAX_MODELS, false, null);
        getModelCount(0, false, new Date());
    }

    private void getModelCount(int n, boolean considerAllStatus, Date lastUpdateTime) {
        String url = apiHostPort + "/score/modeldetails/count?considerAllStatus=" + considerAllStatus;
        if (lastUpdateTime != null) {
            url += "&start=" + dateFormat.format(lastUpdateTime);
        }

        ResponseEntity<Integer> response = oAuth2RestTemplate.exchange(url, HttpMethod.GET, null, Integer.class);
        int modelsCount = response.getBody();
        Assert.assertEquals(modelsCount, n);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "getModelsCountAll", "getModelsCountActive" })
    public void scoreRecords() throws IOException, InterruptedException {
        final String url = apiHostPort + "/score/records";
        Map<TestModelConfiguration, TestModelArtifactDataComposition> models = new HashMap<>();
        TestRegisterModels modelCreator = new TestRegisterModels();

        for (int i = 0; i < MAX_MODELS; i++) {
            String testModelFolderName = "3MulesoftAllRows" + i + "20160314_112802";
            String applicationId = "application_" + i + "1457046993615_3823";
            String modelVersion = "ba99b36-c222-4f93" + i + "-ab8a-6dcc11ce45e9";
            TestModelConfiguration modelConfiguration = new TestModelConfiguration(testModelFolderName, applicationId,
                    modelVersion);
            TestModelArtifactDataComposition modelArtifactDataComposition = modelCreator.createModels(yarnConfiguration,
                    plsRest, tenant, modelConfiguration, customerSpace);
            models.put(modelConfiguration, modelArtifactDataComposition);
            System.out.println("Registered model: " + testModelFolderName);
        }
        final List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList = new ArrayList<>(
                models.entrySet());

        Runnable runnable = new Runnable() {

            @Override
            public void run() {
                try {
                    long timeTaken = 0;
                    long baselineTimeForOneEntry = testScore(url, 1, timeTaken, modelList);

                    long upperBoundForBulkScoring = baselineTimeForOneEntry * MAX_FOLD_FOR_TIME_TAKEN;
                    if (upperBoundForBulkScoring < MIN_UPPER_BOUND) {
                        upperBoundForBulkScoring = MIN_UPPER_BOUND;
                    }

                    if (upperBoundForBulkScoring > MAX_UPPER_BOUND) {
                        upperBoundForBulkScoring = MAX_UPPER_BOUND;
                    }

                    System.out.println("Max time upper bound for bulk scoring: " + upperBoundForBulkScoring);

                    testScore(url, 4, upperBoundForBulkScoring, modelList);
                    testScore(url, 8, upperBoundForBulkScoring, modelList);
                    testScore(url, 12, upperBoundForBulkScoring, modelList);
                    testScore(url, 16, upperBoundForBulkScoring, modelList);
                    testScore(url, 20, upperBoundForBulkScoring, modelList);
                    testScore(url, 40, upperBoundForBulkScoring, modelList);
                    testScore(url, 100, upperBoundForBulkScoring, modelList);
                    testScore(url, 200, upperBoundForBulkScoring, modelList);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        };

        List<Thread> threads = new ArrayList<>();

        for (int i = 0; i < MAX_THREADS; i++) {
            Thread th = new Thread(runnable);
            threadPerfMap.put(th.getName(), new ArrayList<String>());
            th.start();
            threads.add(th);
        }

        for (Thread th : threads) {
            th.join();
        }

        if (shouldPrintPerformanceInfo) {
            for (String threadName : threadPerfMap.keySet()) {
                System.out.println(threadName);
                for (String perf : threadPerfMap.get(threadName)) {
                    System.out.println(perf);
                }
            }
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

    private long testScore(String url, int n, long maxTime,
            List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList) throws IOException {
        try {
            BulkRecordScoreRequest bulkScoreRequest = getBulkScoreRequest(n, modelList);
            ObjectMapper om = new ObjectMapper();
            long timeDuration = System.currentTimeMillis();
            ResponseEntity<List> response = oAuth2RestTemplate.postForEntity(url, bulkScoreRequest, List.class);
            timeDuration = System.currentTimeMillis() - timeDuration;
            System.out.println(n + " => " + timeDuration);
            threadPerfMap.get(Thread.currentThread().getName()).add(n + " => " + timeDuration);
            Assert.assertEquals(response.getBody().size(), n);

            int idx = 0;
            for (Object res : response.getBody()) {
                RecordScoreResponse result = om.readValue(om.writeValueAsString(res), RecordScoreResponse.class);
                Assert.assertEquals(result.getScores().size(), RECORD_MODEL_CARDINALITY);
                for (int j = 0; j < RECORD_MODEL_CARDINALITY; j++) {
                    Assert.assertEquals(result.getScores().get(j).getScore(), EXPECTED_SCORE_99);
                    Assert.assertEquals(result.getScores().get(j).getModelId(),
                            modelList.get((idx + j) % MAX_MODELS).getKey().getModelId());
                }

                Record record = bulkScoreRequest.getRecords().get(idx);
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

                Assert.assertNotNull(result.getWarnings());
                if (idx == 0) {
                    Assert.assertEquals(result.getWarnings().size(), RECORD_MODEL_CARDINALITY);
                    Assert.assertTrue(result.getWarnings().get(0).getDescription().contains(MISSING_FIELD_COUNTRY));
                } else if (idx == 1) {
                    Assert.assertEquals(result.getWarnings().size(), RECORD_MODEL_CARDINALITY);
                    Assert.assertTrue(result.getWarnings().get(0).getDescription().contains(MISSING_FIELD_FIRSTNAME));
                } else {
                    Assert.assertEquals(result.getWarnings().size(), 0);
                }
                idx++;
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

    private BulkRecordScoreRequest getBulkScoreRequest(int n,
            List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList) throws IOException {
        BulkRecordScoreRequest bulkRequest = new BulkRecordScoreRequest();
        bulkRequest.setRule("Dummy Rule");
        bulkRequest.setSource("Dummy Source");

        List<Record> records = generateRecords(n, modelList);

        bulkRequest.setRecords(records);

        return bulkRequest;
    }

    private List<Record> generateRecords(int n,
            List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList) throws IOException {
        List<Record> records = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            Record record = new Record();
            record.setRecordId(UUID.randomUUID().toString());
            ScoreRequest scoreRequest = getScoreRequest();
            Map<String, Object> attributeValues = scoreRequest.getRecord();
            if (i == 0) {
                attributeValues.remove(MISSING_FIELD_COUNTRY);
            } else if (i == 1) {
                attributeValues.remove(MISSING_FIELD_FIRSTNAME);
            }
            record.setAttributeValues(attributeValues);
            List<String> modelIds = new ArrayList<>();

            record.setIdType(SALESFORCE);
            for (int j = 0; j < RECORD_MODEL_CARDINALITY; j++) {
                record.setIdType(Record.LATTICE_ID);
                record.setPerformEnrichment(true);
                modelIds.add(modelList.get((i + j) % MAX_MODELS).getKey().getModelId());
            }
            record.setModelIds(modelIds);

            records.add(record);
        }
        return records;
    }
}
