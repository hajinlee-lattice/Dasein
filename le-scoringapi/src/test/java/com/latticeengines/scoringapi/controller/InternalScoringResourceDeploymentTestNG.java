package com.latticeengines.scoringapi.controller;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.io.Files;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.FieldSource;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.network.exposed.scoringapi.InternalScoringApiInterface;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class InternalScoringResourceDeploymentTestNG extends ScoringApiControllerDeploymentTestNGBase {

    private static final String TEST_MODEL_NAME_PREFIX = "TestInternal3MulesoftAllRows";
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
    private Map<String, List<String>> threadPerfMap = new HashMap<>();
    private boolean shouldPrintPerformanceInfo = true;
    private int baseAllModelCount = 0;
    private int baseAllActiveModelCount = 0;

    @Autowired
    private InternalScoringApiInterface internalScoringApiProxy;

    @Autowired
    private MetadataProxy metadataProxy;

    @Test(groups = "deployment", enabled = true)
    public void getModels() {
        List<Model> models = internalScoringApiProxy.getActiveModels(ModelType.CONTACT, customerSpace.toString());
        Assert.assertEquals(models.size(), 1);
        Assert.assertEquals(models.get(0).getModelId(), MODEL_ID);
        Assert.assertEquals(models.get(0).getName(), MODEL_NAME);
    }

    @Test(groups = "deployment", enabled = true)
    public void getFields() {
        String modelId = MODEL_ID;
        Fields fields = internalScoringApiProxy.getModelFields(modelId, customerSpace.toString());
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
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        ScoreResponse scoreResponse = internalScoringApiProxy.scorePercentileRecord(scoreRequest,
                customerSpace.toString());
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreDebugRecord() throws IOException {
        ScoreRequest scoreRequest = getScoreRequest();
        scoreRequest.setModelId(MODEL_ID);
        DebugScoreResponse scoreResponse = (DebugScoreResponse) internalScoringApiProxy
                .scoreProbabilityRecord(scoreRequest, customerSpace.toString());
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        double difference = Math.abs(scoreResponse.getProbability() - 0.5411256857185404d);
        Assert.assertTrue(difference < 0.1);
    }

    @Test(groups = "deployment", enabled = true)
    public void scoreOutOfRangeRecord() throws IOException {
        URL scoreRequestUrl = ClassLoader.getSystemResource(LOCAL_MODEL_PATH + "outofrange_score_request.json");
        String scoreRecordContents = Files.toString(new File(scoreRequestUrl.getFile()), Charset.defaultCharset());
        ScoreRequest scoreRequest = JsonUtils.deserialize(scoreRecordContents, ScoreRequest.class);

        scoreRequest.setModelId(MODEL_ID);

        DebugScoreResponse scoreResponse = (DebugScoreResponse) internalScoringApiProxy
                .scoreProbabilityRecord(scoreRequest, customerSpace.toString());
        System.out.println(JsonUtils.serialize(scoreResponse));
        Assert.assertEquals(scoreResponse.getScore(), EXPECTED_SCORE_99);
        Assert.assertTrue(scoreResponse.getProbability() > 0.27);
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getModelFieldsAfterScoring() {
        List<Model> models = internalScoringApiProxy.getActiveModels(ModelType.CONTACT, customerSpace.toString());
        for (Model model : models) {
            Fields fields = internalScoringApiProxy.getModelFields(model.getModelId(), customerSpace.toString());
            checkFields(model.getName(), fields, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
        }
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "scoreRecords" })
    public void getPaginatedModels() {
        List<ModelDetail> models = internalScoringApiProxy.getPaginatedModels(new Date(0), true, 0, 50,
                customerSpace.toString());
        checkModelDetails(models, TEST_MODEL_NAME_PREFIX, TestRegisterModels.DISPLAY_NAME_PREFIX);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountAll() {
        baseAllModelCount = getModelCount(1, true, null, false);
    }

    @Test(groups = "deployment", enabled = true)
    public void getModelsCountActive() {
        baseAllActiveModelCount = getModelCount(1, false, null, false);
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
        int modelsCount = internalScoringApiProxy.getModelCount(lastUpdateTime, considerAllStatus,
                customerSpace.toString());
        if (shouldAssert) {
            Assert.assertTrue(modelsCount >= n);
        }
        return n;
    }

    @Test(groups = "deployment", enabled = true, dependsOnMethods = { "getModels", "getModelsCountAll",
            "getModelsCountActive" })
    public void scoreRecords() throws IOException, InterruptedException {
        final String url = apiHostPort + "/scoreinternal/records";
        Map<TestModelConfiguration, TestModelArtifactDataComposition> models = new HashMap<>();
        TestRegisterModels modelCreator = new TestRegisterModels();

        for (int i = 0; i < MAX_MODELS; i++) {
            String testModelFolderName = TEST_MODEL_NAME_PREFIX + i + "20160314_112802";
            String applicationId = "application_" + i + "1457046993615_3823";
            String modelVersion = "ba99b36-c222-4f93" + i + "-ab8a-6dcc11ce45e9";
            TestModelConfiguration modelConfiguration = new TestModelConfiguration(testModelFolderName, applicationId,
                    modelVersion);
            TestModelArtifactDataComposition modelArtifactDataComposition = modelCreator.createModels(yarnConfiguration,
                    plsRest, tenant, modelConfiguration, customerSpace, metadataProxy);
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
            long timeDuration = System.currentTimeMillis();
            List<RecordScoreResponse> response = internalScoringApiProxy.scorePercentileRecords(bulkScoreRequest,
                    customerSpace.toString());
            timeDuration = System.currentTimeMillis() - timeDuration;
            System.out.println(n + " => " + timeDuration);
            threadPerfMap.get(Thread.currentThread().getName()).add(n + " => " + timeDuration);
            Assert.assertEquals(response.size(), n);

            int idx = 0;
            for (RecordScoreResponse result : response) {
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
