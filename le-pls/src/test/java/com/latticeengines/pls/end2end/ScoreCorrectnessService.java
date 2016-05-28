package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.python.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;
import org.testng.Assert;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.StringUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Field;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.scoringapi.exposed.ScoreCorrectnessArtifacts;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;

@Component
public class ScoreCorrectnessService {

    private static final Log log = LogFactory.getLog(ScoreCorrectnessService.class);
    private static final int TIMEOUT_IN_MIN = 60;
    private static final int THREADPOOL_SIZE = 64;
    private static final double ACCEPTABLE_PERCENT_DIFFERENCE = 1.0;
    private static final double THRESHOLD = 0.000001;
    private RestTemplate scoringRestTemplate = new RestTemplate();

    @Value("${pls.scoringapi.rest.endpoint.hostport}")
    private String scoreApiHostPort;

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @Autowired
    private Configuration yarnConfiguration;

    @Autowired
    private ModelRetriever modelRetriever;

    private Set<String> notPredictorFields = new HashSet<>();

    @PostConstruct
    public void init() {
        notPredictorFields.add(InterfaceName.Id.name());
        notPredictorFields.add(InterfaceName.Email.name());
        notPredictorFields.add(InterfaceName.Event.name());
        notPredictorFields.add(InterfaceName.CompanyName.name());
        notPredictorFields.add(InterfaceName.City.name());
        notPredictorFields.add(InterfaceName.State.name());
        notPredictorFields.add(InterfaceName.Country.name());
        notPredictorFields.add(InterfaceName.PostalCode.name());
        notPredictorFields.add(InterfaceName.CreatedDate.name());
        notPredictorFields.add(InterfaceName.LastModifiedDate.name());
        notPredictorFields.add(InterfaceName.FirstName.name());
        notPredictorFields.add(InterfaceName.LastName.name());
        notPredictorFields.add(InterfaceName.Title.name());
        notPredictorFields.add(InterfaceName.IsClosed.name());
        notPredictorFields.add(InterfaceName.StageName.name());
        notPredictorFields.add(InterfaceName.PhoneNumber.name());

    }

    public void analyzeScores(String tenantId, String pathToModelInputCsv, String modelId, int numRecordsToScore)
            throws IOException {
        String accessToken = oauth2RestApiProxy.createOAuth2AccessToken(tenantId).getValue();
        Oauth2HeaderHttpRequestInterceptor interceptor = new Oauth2HeaderHttpRequestInterceptor(accessToken);
        scoringRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));

        ScoreCorrectnessArtifacts artifacts = modelRetriever
                .retrieveScoreCorrectnessArtifactsFromHdfs(CustomerSpace.parse(tenantId), modelId);

        Map<String, Double> expectedScores = getExpectedScoresFromScoredTxt(artifacts, numRecordsToScore);
        Map<String, Map<String, Object>> expectedRecords = getExpectedRecords(artifacts);
        Map<String, Map<String, Object>> matchedRecords = getExpectedMatchedRecords(artifacts);
        Map<String, FieldSchema> schema = artifacts.getFieldSchemas();
        Fields fieldsFromScoreApi = getModelingFields(modelId);
        Map<String, Map<String, Object>> inputRecords = getInputRecordsFromInputCsv(artifacts, pathToModelInputCsv,
                expectedScores, fieldsFromScoreApi.getFields());
        Map<String, DebugScoreResponse> scoreResponses = scoreRecords(modelId, inputRecords);
        Map<String, ComparedRecord> differentRecords = compareExpectedVsScoredRecords(schema, expectedScores,
                expectedRecords, matchedRecords, scoreResponses);
        outputResults(scoreResponses.size(), differentRecords);

        double percentDifference = (double) differentRecords.size() / (double) scoreResponses.size() * 100.0;
        System.out.println("PercentDifference:" + percentDifference);
        Assert.assertTrue(percentDifference <= ACCEPTABLE_PERCENT_DIFFERENCE, "percent different " + percentDifference
                + " exceeds the maximum allowance " + ACCEPTABLE_PERCENT_DIFFERENCE);
        double percentScored = (double) scoreResponses.size() / (double) numRecordsToScore * 100.0;
        log.info("Percent Scored:" + percentScored);
        Assert.assertTrue(percentScored >= 90.0,
                String.format("Actual scored records less than 90 percent, actual:%d, expected:%d",
                        scoreResponses.size(), numRecordsToScore));
    }

    private void outputResults(int totalCompared, Map<String, ComparedRecord> result) {
        System.out.println("***** Details of Delta Records *****");
        for (String id : result.keySet()) {
            ComparedRecord compared = result.get(id);
            System.out.println(String.format("ID:%s Expected:%s ScoreApi:%s Delta:%s", id, compared.getExpectedScore(),
                    compared.getScoreApiScore(), compared.getScoreDifference()));
            System.out.println(JsonUtils.serialize(compared));
        }

        int num5 = 0, num4 = 0, num3 = 0, num2 = 0, numRecordsWithMatchConflicts = 0,
                numRecordsWithTransformConflicts = 0, totalMatchConflicts = 0, totalTransformConflicts = 0;
        for (ComparedRecord compared : result.values()) {
            totalMatchConflicts += compared.getNumMatchFieldConflicts();
            totalTransformConflicts += compared.getNumTransformFieldConflicts();

            if (compared.getNumMatchFieldConflicts() > 0) {
                numRecordsWithMatchConflicts++;
            }
            if (compared.getNumTransformFieldConflicts() > 0) {
                numRecordsWithTransformConflicts++;
            }
            if (compared.getScoreDifference() > 0.00001) {
                num5++;
            }
            if (compared.getScoreDifference() > 0.0001) {
                num4++;
            }
            if (compared.getScoreDifference() > 0.001) {
                num3++;
            }
            if (compared.getScoreDifference() > 0.01) {
                num2++;
            }

        }

        System.out.println("***** Summary of Delta Records *****");
        for (String id : result.keySet()) {
            ComparedRecord compared = result.get(id);
            System.out.println(String.format(
                    "ID:%s Expected:%s ScoreApi:%s Delta:%s NumMatchConflicts:%d NumTranformConflicts:%s ScoreWarnings:%s",
                    id, compared.getExpectedScore(), compared.getScoreApiScore(), compared.getScoreDifference(),
                    compared.getNumMatchFieldConflicts(), compared.getNumTransformFieldConflicts(),
                    JsonUtils.serialize(compared.getWarnings())));
        }

        System.out.println("***** Overall Summary *****");
        System.out
                .println(generateThresholdSummary(totalCompared, result.size(), THRESHOLD, numRecordsWithMatchConflicts,
                        numRecordsWithTransformConflicts, totalMatchConflicts, totalTransformConflicts));
        System.out.println(generateSummary(totalCompared, num5, 0.00001));
        System.out.println(generateSummary(totalCompared, num4, 0.0001));
        System.out.println(generateSummary(totalCompared, num3, 0.001));
        System.out.println(generateSummary(totalCompared, num2, 0.01));

    }

    private String generateThresholdSummary(int totalRecords, int different, double threshold,
            int numRecordsWithMatchConflicts, int numRecordsWithTransformConflicts, int totalMatchConflicts,
            int totalTransformConflicts) {
        return String.format(
                "threshold:%f recordsCompared:%d different:%d percentDifferent:%3.1f differentRecordsWithMatchConflicts:%d differentRecordsWithTransformConflicts:%d avgMatchConflictsPerMatchConflictedRecord:%d avgTransformConflictsPerTransformConflictedRecord:%d",
                threshold, totalRecords, different, (float) different / (float) totalRecords * 100.0,
                numRecordsWithMatchConflicts, numRecordsWithTransformConflicts,
                numRecordsWithMatchConflicts == 0 ? 0 : totalMatchConflicts / numRecordsWithMatchConflicts,
                numRecordsWithTransformConflicts == 0 ? 0 : totalTransformConflicts / numRecordsWithTransformConflicts);
    }

    private String generateSummary(int totalRecords, int different, double threshold) {
        return String.format("threshold:%f recordsCompared:%d different:%d percentDifferent:%3.1f", threshold,
                totalRecords, different, (float) different / (float) totalRecords * 100.0);
    }

    private Map<String, ComparedRecord> compareExpectedVsScoredRecords(Map<String, FieldSchema> schema,
            Map<String, Double> expectedScores, Map<String, Map<String, Object>> expectedRecords,
            Map<String, Map<String, Object>> matchedRecords, Map<String, DebugScoreResponse> scoredResponses) {
        Map<String, ComparedRecord> deltas = new HashMap<>();
        for (String id : expectedScores.keySet()) {
            DebugScoreResponse response = scoredResponses.get(id);
            if (response == null) {
                // Skip analysis of this record since failed scoring it.
                continue;
            }
            double difference = Math.abs(expectedScores.get(id) - response.getProbability());
            if (difference > THRESHOLD) {
                Map<String, Object> expectedMatchedRecord = matchedRecords.get(id);
                Map<String, Object> scoreMatchedRecord = response.getMatchedRecord();
                Triple<List<FieldConflict>, List<String>, List<String>> matchDiffs = diffRecords(expectedMatchedRecord,
                        scoreMatchedRecord, schema, false);

                Map<String, Object> expectedRecord = expectedRecords.get(id);
                Map<String, Object> scoreRecord = response.getTransformedRecord();
                Triple<List<FieldConflict>, List<String>, List<String>> transformDiffs = diffRecords(expectedRecord,
                        scoreRecord, schema, true);

                ComparedRecord comparedRecord = new ComparedRecord();
                comparedRecord.setId(id);

                comparedRecord.setExpectedScore(expectedScores.get(id));
                comparedRecord.setScoreApiScore(response.getProbability());
                comparedRecord.setScoreDifference(difference);
                comparedRecord.setWarnings(response.getWarnings());

                comparedRecord.setExpectedMatchedRecord(matchedRecords.get(id));
                comparedRecord.setScoreApiMatchedRecord(response.getMatchedRecord());
                comparedRecord.setMatchedFieldConflicts(matchDiffs.getLeft());
                comparedRecord.setNumMatchFieldConflicts(matchDiffs.getLeft().size());
                comparedRecord.setScoreApiMatchExtraFields(matchDiffs.getMiddle());
                comparedRecord.setScoreApiMatchMissingFields(matchDiffs.getRight());

                comparedRecord.setTransformFieldConflicts(transformDiffs.getLeft());
                comparedRecord.setExpectedTransformedRecord(expectedRecord);
                comparedRecord.setScoreApiTransformedRecord(scoreRecord);
                comparedRecord.setNumTransformFieldConflicts(transformDiffs.getLeft().size());
                comparedRecord.setScoreApiTransformExtraFields(transformDiffs.getMiddle());
                comparedRecord.setScoreApiTransformMissingFields(transformDiffs.getRight());

                deltas.put(id, comparedRecord);
            }
        }

        return deltas;
    }

    private Triple<List<FieldConflict>, List<String>, List<String>> diffRecords(Map<String, Object> expectedRecord,
            Map<String, Object> scoreRecord, Map<String, FieldSchema> schema, boolean isDiffTransform) {
        List<FieldConflict> conflicts = new ArrayList<>();
        List<String> extraFields = new ArrayList<>();
        List<String> missingFields = new ArrayList<>();

        if (expectedRecord == null || expectedRecord.keySet().isEmpty()) {
            return Triple.of(conflicts, extraFields, missingFields);
        }
        Set<String> expectedKeys = new HashSet<>(expectedRecord.keySet());
        Set<String> scoredKeys = new HashSet<>(scoreRecord.keySet());

        scoredKeys.removeAll(expectedKeys);
        if (!scoredKeys.isEmpty()) {
            extraFields.addAll(scoredKeys);
        }

        scoredKeys.clear();
        scoredKeys.addAll(scoreRecord.keySet());

        expectedKeys.removeAll(scoredKeys);
        if (!expectedKeys.isEmpty()) {
            missingFields.addAll(expectedKeys);
        }

        Set<String> intersection = new HashSet<>(expectedRecord.keySet());
        intersection.retainAll(scoreRecord.keySet());
        for (String field : intersection) {
            if (isDiffTransform && notPredictorFields.contains(field)) {
                continue;
            }
            try {
                String expectedValueString = String.valueOf(expectedRecord.get(field));
                String scoreValueString = String.valueOf(scoreRecord.get(field));
                SimpleEntry<Object, Object> parsed = parseObjects(expectedValueString, scoreValueString);
                Object expectedValue = parsed.getKey();
                Object scoreValue = parsed.getValue();
                if (!expectedValue.equals(scoreValue)) {
                    FieldConflict conflict = new FieldConflict();
                    conflict.setFieldName(field);
                    conflict.setExpectedValue(expectedValue);
                    conflict.setScoreApiValue(scoreValue);
                    conflicts.add(conflict);
                }
            } catch (RuntimeException e) {
                log.error("Problem parsing field: " + field);
                throw e;
            }
        }

        return Triple.of(conflicts, extraFields, missingFields);
    }

    private SimpleEntry<Object, Object> parseObjects(String expected, String scoreApi) {
        Object parsedExpected = expected;
        Object parsedScoreApi = scoreApi;
        try {
            double doubleExpected = Double.parseDouble(expected);
            double doubleScoreApi = Double.parseDouble(scoreApi);
            parsedExpected = doubleExpected;
            parsedScoreApi = doubleScoreApi;
            double difference = Math.abs(doubleExpected - doubleScoreApi);
            if (difference < THRESHOLD) {
                // difference is so minor just count these two as the same
                parsedScoreApi = parsedExpected;
            }
        } catch (NumberFormatException edouble) {
            try {
                parsedExpected = Long.parseLong(expected);
                parsedScoreApi = Long.parseLong(scoreApi);
            } catch (NumberFormatException elong) {
                parsedExpected = expected;
                parsedScoreApi = scoreApi;
            }
        }
        return new SimpleEntry<>(parsedExpected, parsedScoreApi);
    }

    private Map<String, DebugScoreResponse> scoreRecords(String modelId,
            Map<String, Map<String, Object>> inputRecords) {
        final Queue<String> inputQueue = new ConcurrentLinkedQueue<>();
        Set<String> problemScores = new ConcurrentSkipListSet<>();
        Map<String, DebugScoreResponse> responses = new ConcurrentHashMap<>();
        ExecutorService scoreExecutorService = Executors.newFixedThreadPool(THREADPOOL_SIZE);
        final AtomicInteger counter = new AtomicInteger(0);

        inputQueue.addAll(inputRecords.keySet());
        final int inputQueueSize = inputQueue.size();
        for (int i = 0; i < THREADPOOL_SIZE; i++) {
            scoreExecutorService.execute(new ScoreApiCallable(scoreApiHostPort, scoringRestTemplate, modelId,
                    inputRecords, inputQueue, problemScores, responses, inputQueueSize, counter));
        }

        scoreExecutorService.shutdown();
        try {
            scoreExecutorService.awaitTermination(TIMEOUT_IN_MIN, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
        }

        if (!problemScores.isEmpty()) {
            log.error("Problem scoring these record ids: " + JsonUtils.serialize(problemScores));
        }
        return responses;
    }

    private static class ScoreApiCallable implements Callable<Void>, Runnable {

        private String scoreApiHostPort;
        private RestTemplate scoringRestTemplate;
        private String modelId;
        private Map<String, Map<String, Object>> inputRecords;
        private Queue<String> inputQueue;
        private Set<String> problemScores;
        private Map<String, DebugScoreResponse> responses;
        private int initialInputQueueSize;
        private AtomicInteger counter;

        public ScoreApiCallable(String scoreApiHostPort, RestTemplate scoringRestTemplate, String modelId,
                Map<String, Map<String, Object>> inputRecords, Queue<String> inputQueue, Set<String> problemScores,
                Map<String, DebugScoreResponse> responses, int initialInputQueueSize, AtomicInteger counter) {
            super();
            this.scoreApiHostPort = scoreApiHostPort;
            this.scoringRestTemplate = scoringRestTemplate;
            this.modelId = modelId;
            this.inputRecords = inputRecords;
            this.problemScores = problemScores;
            this.responses = responses;
            this.inputQueue = inputQueue;
            this.initialInputQueueSize = initialInputQueueSize;
            this.counter = counter;
        }

        @Override
        public Void call() throws Exception {
            while (true) {
                String id = inputQueue.poll();
                if (id == null) {
                    break;
                }
                try {
                    DebugScoreResponse response = score(inputRecords.get(id), modelId);
                    log.info((String.format("Scored record id %s #%d out of %d", id, counter.incrementAndGet(),
                            initialInputQueueSize)));
                    responses.put(id, response);
                } catch (Exception e) {
                    e.printStackTrace();
                    problemScores.add(id);
                }
            }
            return null;
        }

        private DebugScoreResponse score(Map<String, Object> record, String modelId) {
            ScoreRequest request = new ScoreRequest();
            request.setModelId(modelId);
            request.setRecord(record);
            DebugScoreResponse response = scoringRestTemplate.postForObject(scoreApiHostPort + "/score/record/debug",
                    request, DebugScoreResponse.class);
            return response;
        }

        @Override
        public void run() {
            try {
                call();
            } catch (Exception e) {
            }
        }
    }

    private Map<String, Map<String, Object>> getInputRecordsFromInputCsv(ScoreCorrectnessArtifacts artifacts,
            String pathToModelInputCsv, Map<String, Double> expectedScores, List<Field> fields) throws IOException {
        Map<String, Map<String, Object>> records = new HashMap<>();
        Set<String> ids = expectedScores.keySet();
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        ClassPathResource csvResource = new ClassPathResource(pathToModelInputCsv);
        try (CSVParser parser = new CSVParser(new InputStreamReader(new BOMInputStream(csvResource.getInputStream())),
                format)) {
            for (CSVRecord csvRecord : parser) {
                String id = csvRecord.get(artifacts.getIdField());
                if (ids.contains(id)) {
                    Map<String, Object> scoreRecord = new HashMap<>();
                    for (Field field : fields) {
                        String fieldName = field.getFieldName();
                        scoreRecord.put(fieldName, csvRecord.get(fieldName));
                        if (csvRecord.get(fieldName).equals("")) {
                            scoreRecord.put(fieldName, null);
                        }
                    }
                    records.put(id, scoreRecord);
                }
            }
        }
        return records;
    }

    private Map<String, Map<String, Object>> getExpectedMatchedRecords(ScoreCorrectnessArtifacts artifacts)
            throws IOException {
        Map<String, Map<String, Object>> matchedRecords = new HashMap<>();
        List<GenericRecord> avroRecords = AvroUtils.getDataFromGlob(yarnConfiguration,
                artifacts.getPathToSamplesAvro() + "allTest-r-00000.avro");
        Schema schema = avroRecords.get(0).getSchema();
        List<Schema.Field> fields = schema.getFields();

        for (GenericRecord avroRecord : avroRecords) {
            Map<String, Object> record = new HashMap<>();
            Object idFieldValue = null;
            for (Schema.Field field : fields) {
                String fieldName = field.name();
                Object fieldValue = avroRecord.get(fieldName);
                record.put(fieldName, fieldValue);
                if (fieldName.equals(artifacts.getIdField())) {
                    idFieldValue = fieldValue;
                }
            }
            if (StringUtils.objectIsNullOrEmptyString(idFieldValue)) {
                log.warn("Skipping this record because missing ID field value " + JsonUtils.serialize(record));
                continue;
            } else {
                matchedRecords.put(String.valueOf(idFieldValue), record);
            }

        }

        return matchedRecords;
    }

    private Map<String, Map<String, Object>> getExpectedRecords(ScoreCorrectnessArtifacts artifacts)
            throws IOException {
        Map<String, Map<String, Object>> records = new HashMap<>();
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        try (CSVParser parser = CSVParser.parse(artifacts.getExpectedRecords(), format)) {
            String idFieldValue = null;
            for (CSVRecord csvRecord : parser) {
                for (String fieldName : parser.getHeaderMap().keySet()) {
                    if (fieldName.equals(artifacts.getIdField())) {
                        idFieldValue = csvRecord.get(fieldName);
                    }
                }
                Map<String, Object> record = new HashMap<>();
                record.putAll(csvRecord.toMap());
                if (Strings.isNullOrEmpty(idFieldValue)) {
                    log.warn("Skipping this record because missing ID field value " + JsonUtils.serialize(record));
                    continue;
                } else {
                    records.put(idFieldValue, record);
                }
            }
        }

        return records;
    }

    // omit duplicate ID's from the test set
    private Map<String, Double> getExpectedScoresFromScoredTxt(ScoreCorrectnessArtifacts artifacts,
            int numRecordsToScore) {
        Map<String, Double> expectedScores = new HashMap<>();
        Map<String, Double> scoresToTest = new HashMap<>();

        String[] rows = artifacts.getScoredTxt().split("\n");

        Set<String> ids = new HashSet<>();
        Set<String> duplicateIds = new HashSet<>();
        for (String row : rows) {
            String[] columns = row.split(",");
            assert (columns.length == 2);
            String id = columns[0];
            Double score = Double.valueOf(columns[1]);
            expectedScores.put(id, score);

            if (ids.contains(id)) {
                duplicateIds.add(id);
            } else {
                ids.add(id);
            }
        }

        log.warn(String.format("Found %d duplicate ids: %s", duplicateIds.size(), JsonUtils.serialize(duplicateIds)));

        int rowNum = 1;
        for (String id : expectedScores.keySet()) {
            if (!duplicateIds.contains(id)) {
                scoresToTest.put(id, expectedScores.get(id));
            }
            rowNum++;
            if (rowNum > numRecordsToScore) {
                break;
            }
        }

        return scoresToTest;
    }

    private Fields getModelingFields(String modelId) {
        Fields fields = scoringRestTemplate.getForObject(scoreApiHostPort + "/score/models/" + modelId + "/fields",
                Fields.class);
        return fields;
    }

}
