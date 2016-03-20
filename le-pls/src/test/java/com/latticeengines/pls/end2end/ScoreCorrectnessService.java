package com.latticeengines.pls.end2end;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.input.BOMInputStream;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.python.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ClassPathResource;
import org.springframework.http.client.ClientHttpRequestInterceptor;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import com.joestelmach.natty.DateGroup;
import com.joestelmach.natty.Parser;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.scoringapi.FieldSchema;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;
import com.latticeengines.scoringapi.exposed.DebugScoreResponse;
import com.latticeengines.scoringapi.exposed.Field;
import com.latticeengines.scoringapi.exposed.Fields;
import com.latticeengines.scoringapi.exposed.ScoreCorrectnessArtifacts;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.model.ModelRetriever;

@Component
public class ScoreCorrectnessService {

    private static final Log log = LogFactory.getLog(ScoreCorrectnessService.class);
    private static final int NUM_LEADS_TO_SCORE = 50;
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

    public void analyzeScores(String tenantId, String pathToModelInputCsv, String modelId) throws IOException {
        String accessToken = oauth2RestApiProxy.createOAuth2AccessToken(tenantId).getValue();
        Oauth2HeaderHttpRequestInterceptor interceptor = new Oauth2HeaderHttpRequestInterceptor(accessToken);
        scoringRestTemplate.setInterceptors(Arrays.asList(new ClientHttpRequestInterceptor[] { interceptor }));

        ScoreCorrectnessArtifacts artifacts = modelRetriever.retrieveScoreCorrectnessArtifactsFromHdfs(
                CustomerSpace.parse(tenantId), modelId);

        Map<String, Double> expectedScores = getExpectedScoresFromScoredTxt(artifacts);
        Map<String, Map<String, String>> expectedRecords = getExpectedRecords(artifacts);
        Map<String, FieldSchema> schema = artifacts.getDataScienceDataComposition().fields;
        Fields fieldsFromScoreApi = getModelingFields(modelId);
        Map<String, Map<String, Object>> inputRecords = getInputRecordsFromInputCsv(artifacts, pathToModelInputCsv,
                expectedScores, fieldsFromScoreApi.getFields());
        Map<String, DebugScoreResponse> scoreResponses = scoreRecords(modelId, inputRecords);
        Map<String, ComparedRecord> differentRecords = compareExpectedVsScoredRecords(schema, expectedScores,
                expectedRecords, scoreResponses);
        outputResults(expectedScores.size(), differentRecords);

        Assert.assertTrue(differentRecords.isEmpty());
    }

    private void outputResults(int totalCompared, Map<String, ComparedRecord> result) {
        String summaryThreshold = generateSummary(totalCompared, result.size(), THRESHOLD);
        System.out.println(summaryThreshold);
        System.out.println("Records with different scores:");
        for (String id : result.keySet()) {
            ComparedRecord compared = result.get(id);
            System.out.println(String.format("ID:%s Expected:%s ScoreApi:%s Delta:%s", id, compared.getExpectedScore(),
                    compared.getScoreApiScore(), compared.getScoreDifference()));
        }
        for (String id : result.keySet()) {
            ComparedRecord compared = result.get(id);
            System.out.println(String.format("ID:%s Expected:%s ScoreApi:%s Delta:%s", id, compared.getExpectedScore(),
                    compared.getScoreApiScore(), compared.getScoreDifference()));
            System.out.println(JsonUtils.serialize(compared));
        }

        System.out.println(summaryThreshold);

        int num5 = 0, num4 = 0, num3 = 0, num2 = 0;
        for (ComparedRecord compared : result.values()) {
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

        System.out.println(generateSummary(totalCompared, num5, 0.00001));
        System.out.println(generateSummary(totalCompared, num4, 0.0001));
        System.out.println(generateSummary(totalCompared, num3, 0.001));
        System.out.println(generateSummary(totalCompared, num2, 0.01));

    }

    private String generateSummary(int totalRecords, int different, double threshold) {
        return String.format(
                "Number of records compared:%d different:%d percent-different:%3.1f threshold:%f", totalRecords,
                different, (float) different / (float) totalRecords * 100.0, threshold);
    }

    private Map<String, ComparedRecord> compareExpectedVsScoredRecords(Map<String, FieldSchema> schema,
            Map<String, Double> expectedScores, Map<String, Map<String, String>> expectedRecords,
            Map<String, DebugScoreResponse> scoredResponses) {
        Map<String, ComparedRecord> deltas = new HashMap<>();
        for (String id : expectedScores.keySet()) {
            DebugScoreResponse response = scoredResponses.get(id);
            double difference = Math.abs(expectedScores.get(id) - response.getProbability());
            if (difference > THRESHOLD) {
                Map<String, String> expectedRecord = expectedRecords.get(id);
                Map<String, Object> scoreRecord = response.getTransformedRecord();
                Triple<List<FieldConflict>, List<String>, List<String>> diffs = diffRecords(expectedRecord,
                        scoreRecord, schema);
                ComparedRecord comparedRecord = new ComparedRecord();
                comparedRecord.setFieldConflicts(diffs.getLeft());
                comparedRecord.setId(id);
                comparedRecord.setExpectedTransformedRecord(expectedRecord);
                comparedRecord.setExpectedScore(expectedScores.get(id));
                comparedRecord.setScoreApiExtraFields(diffs.getMiddle());
                comparedRecord.setScoreApiMissingFields(diffs.getRight());
                comparedRecord.setScoreApiTransformedRecord(scoreRecord);
                comparedRecord.setScoreApiScore(response.getProbability());
                comparedRecord.setScoreDifference(difference);
                deltas.put(id, comparedRecord);
            }
        }

        return deltas;
    }

    private Triple<List<FieldConflict>, List<String>, List<String>> diffRecords(Map<String, String> expectedRecord,
            Map<String, Object> scoreRecord, Map<String, FieldSchema> schema) {
        List<FieldConflict> conflicts = new ArrayList<>();
        List<String> extraFields = new ArrayList<>();
        List<String> missingFields = new ArrayList<>();

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
            // FieldSchema fieldSchema = schema.get(field);
            try {
                String expectedValueString = String.valueOf(expectedRecord.get(field));
                String scoreValueString = String.valueOf(scoreRecord.get(field));
                SimpleEntry<Object, Object> parsed = parseObjects(expectedValueString, scoreValueString);
                Object expectedValue = parsed.getKey();
                Object scoreValue = parsed.getValue();
                // if (fieldSchema.source.equals(FieldSource.REQUEST)) {
                // expectedValue = FieldType.parse(fieldSchema.type,
                // expectedRecord.get(field));
                // scoreValue = FieldType.parse(fieldSchema.type,
                // scoreRecord.get(field));
                // } else {

                // }
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
            parsedExpected = Double.parseDouble(expected);
            parsedScoreApi = Double.parseDouble(scoreApi);
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

    private Map<String, DebugScoreResponse> scoreRecords(String modelId, Map<String, Map<String, Object>> inputRecords) {
        Map<String, DebugScoreResponse> responses = new HashMap<>();
        for (String id : inputRecords.keySet()) {
            DebugScoreResponse response = score(inputRecords.get(id), modelId);
            responses.put(id, response);
        }
        return responses;
    }

    private DebugScoreResponse score(Map<String, Object> record, String modelId) {
        ScoreRequest request = new ScoreRequest();
        request.setModelId(modelId);
        request.setRecord(record);
        DebugScoreResponse response = scoringRestTemplate.postForObject(scoreApiHostPort + "/score/record/debug",
                request, DebugScoreResponse.class);
        return response;
    }

    private Map<String, Map<String, Object>> getInputRecordsFromInputCsv(ScoreCorrectnessArtifacts artifacts,
            String pathToModelInputCsv, Map<String, Double> expectedScores, List<Field> fields) throws IOException {
        Map<String, Map<String, Object>> records = new HashMap<>();
        Set<String> ids = expectedScores.keySet();
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        ClassPathResource csvResource = new ClassPathResource(pathToModelInputCsv);
        try (CSVParser parser = new CSVParser(new InputStreamReader(new BOMInputStream(csvResource.getInputStream())),
                format)) {
            Parser dateParser = new Parser();
            for (CSVRecord csvRecord : parser) {
                String id = csvRecord.get(artifacts.getIdField());
                if (ids.contains(id)) {
                    Map<String, Object> scoreRecord = new HashMap<>();
                    for (Field field : fields) {
                        String fieldName = field.getFieldName();
                        scoreRecord.put(fieldName, csvRecord.get(fieldName));
                        if (csvRecord.get(fieldName).equals("")) {
                            scoreRecord.put(fieldName, null);
                        } else if (fieldName.equalsIgnoreCase(InterfaceName.CreatedDate.name())
                                || fieldName.equalsIgnoreCase(InterfaceName.LastModifiedDate.name())) {
                            // Can remove this conversion case once we set
                            // ApprovedUsage to none of these fields.
                            List<DateGroup> groups = dateParser.parse(csvRecord.get(fieldName));
                            List<Date> dates = groups.get(0).getDates();
                            scoreRecord.put(fieldName, dates.get(0).getTime());
                        }
                    }
                    records.put(id, scoreRecord);
                }
            }
        }
        return records;
    }

    private Map<String, Map<String, String>> getExpectedRecords(ScoreCorrectnessArtifacts artifacts) throws IOException {
        Map<String, Map<String, String>> records = new HashMap<>();
        CSVFormat format = CSVFormat.RFC4180.withHeader().withDelimiter(',');
        try (CSVParser parser = CSVParser.parse(artifacts.getExpectedRecords(), format)) {
            String idFieldValue = null;
            for (CSVRecord csvRecord : parser) {
                for (String fieldName : parser.getHeaderMap().keySet()) {
                    if (fieldName.equals(artifacts.getIdField())) {
                        idFieldValue = csvRecord.get(fieldName);
                    }
                }
                Map<String, String> record = csvRecord.toMap();
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
    private Map<String, Double> getExpectedScoresFromScoredTxt(ScoreCorrectnessArtifacts artifacts) {
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
            if (rowNum > NUM_LEADS_TO_SCORE) {
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
