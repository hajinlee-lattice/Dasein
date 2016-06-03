package com.latticeengines.scoringapi.controller;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.testng.Assert;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.proxy.exposed.metadata.MetadataProxy;
import com.latticeengines.scoringapi.functionalframework.ScoringApiControllerDeploymentTestNGBase;

public class ScoringResourceDeploymentTestNGBase extends ScoringApiControllerDeploymentTestNGBase {
    protected static final String TEST_MODEL_NAME_PREFIX = "TestInternal3MulesoftAllRows";
    protected static final String SALESFORCE = "SALESFORCE";
    protected static final String MISSING_FIELD_COUNTRY = "Country";
    protected static final String MISSING_FIELD_FIRSTNAME = "FirstName";
    protected static final int MAX_FOLD_FOR_TIME_TAKEN = 10;
    // allow atleast 80 seconds of upper bound for bulk scoring api to make sure
    // that this testcase can work if performance is fine. If performance
    // degrades a lot in future then this limit will correctly fail the testcase
    protected static final long MIN_UPPER_BOUND = TimeUnit.SECONDS.toMillis(80);
    protected static final long MAX_UPPER_BOUND = TimeUnit.SECONDS.toMillis(120);
    protected static final double EXPECTED_SCORE_99 = 99.0d;
    protected static final int MAX_THREADS = 1;
    protected static final int RECORD_MODEL_CARDINALITY = 2;
    protected static final int MAX_MODELS = 20;
    protected volatile Throwable exception = null;
    protected Map<String, List<String>> threadPerfMap = new HashMap<>();
    protected boolean shouldPrintPerformanceInfo = true;
    protected int baseAllModelCount = 0;
    protected int baseAllActiveModelCount = 0;

    protected static final String DATE_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssZ";
    protected static final String UTC = "UTC";
    protected static final SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_STRING);

    static {
        dateFormat.setTimeZone(TimeZone.getTimeZone(UTC));
    }

    @Autowired
    protected MetadataProxy metadataProxy;

    protected List<Record> generateRecords(int n,
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

            List<String> modelIds = new ArrayList<>();

            record.setIdType(SALESFORCE);
            for (int j = 0; j < RECORD_MODEL_CARDINALITY; j++) {
                record.setIdType(Record.LATTICE_ID);
                record.setPerformEnrichment(true);
                modelIds.add(modelList.get((i + j) % MAX_MODELS).getKey().getModelId());
            }

            Map<String, Map<String, Object>> modelAttributeValuesMap = new HashMap<>();

            for (String modelId : modelIds) {
                modelAttributeValuesMap.put(modelId, attributeValues);
            }

            record.setModelAttributeValuesMap(modelAttributeValuesMap);

            records.add(record);
        }
        return records;
    }

    protected String getExceptionTrace(Throwable exception2) {
        StringWriter writer = new StringWriter();
        PrintWriter printWriter = new PrintWriter(writer);
        exception.printStackTrace(printWriter);
        printWriter.flush();

        String stackTrace = writer.toString();
        return stackTrace.toString();
    }

    protected long testScore(String url, int n, long maxTime,
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
                Set<String> modelsSubset = new HashSet<>();

                for (int j = 0; j < RECORD_MODEL_CARDINALITY; j++) {
                    modelsSubset.add(modelList.get((idx + j) % MAX_MODELS).getKey().getModelId());
                }

                for (int j = 0; j < RECORD_MODEL_CARDINALITY; j++) {
                    Assert.assertEquals(result.getScores().get(j).getScore(), EXPECTED_SCORE_99);
                    String modelId = result.getScores().get(j).getModelId();
                    Assert.assertTrue(modelsSubset.contains(modelId));
                    modelsSubset.remove(modelId);
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

    protected BulkRecordScoreRequest getBulkScoreRequest(int n,
            List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList) throws IOException {
        BulkRecordScoreRequest bulkRequest = new BulkRecordScoreRequest();
        bulkRequest.setRule("Dummy Rule");
        bulkRequest.setSource("Dummy Source");

        List<Record> records = generateRecords(n, modelList);

        bulkRequest.setRecords(records);

        return bulkRequest;
    }

    protected Runnable createScoringRunnable(final String url,
            final List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList) {
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
        return runnable;
    }

    protected List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> createModelList()
            throws IOException {
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
        return modelList;
    }

    protected void runScoringTest(final String url) throws IOException, InterruptedException {
        final List<Entry<TestModelConfiguration, TestModelArtifactDataComposition>> modelList = createModelList();

        Runnable runnable = createScoringRunnable(url, modelList);

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

}
