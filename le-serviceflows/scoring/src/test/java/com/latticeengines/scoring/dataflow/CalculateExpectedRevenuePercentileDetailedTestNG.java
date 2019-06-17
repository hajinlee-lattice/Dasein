package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.dataflow.runtime.cascading.cdl.CalculateFittedExpectedRevenueFunction;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class CalculateExpectedRevenuePercentileDetailedTestNG extends ScoringServiceFlowsDataFlowFunctionalTestNGBase {

    String evModelGuid = "ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz";

    @Test(groups = "functional")
    public void testCalculationExpectedRevenuePercentile() {
        CalculateExpectedRevenuePercentileParameters parameters = prepareInputWithExpectedRevenue();
        executeDataFlow(parameters);
        verifyResults();
    }

    private void verifyResults() {
        List<GenericRecord> inputRecords = readInput("detailed");
        List<GenericRecord> expectedResultsRecords = readInput("expectedResult");
        List<GenericRecord> outputRecords = readOutput();

        assertEquals(outputRecords.size(), inputRecords.size());
        assertEquals(outputRecords.size(), expectedResultsRecords.size());
        String[] modelGuids = { evModelGuid, };

        Map<String, List<GenericRecord>> modelRecordMap = calculateModelRecordMap(outputRecords, modelGuids);
        Map<String, List<GenericRecord>> modelRecordMapExpectedResult = calculateModelRecordMap(expectedResultsRecords,
                modelGuids);

        assertEquals(9465, modelRecordMap.get(evModelGuid).size());
        assertEquals(9465, modelRecordMapExpectedResult.get(evModelGuid).size());

        String[] evModelGuids = { evModelGuid, };

        for (String modelGuid : evModelGuids) {
            verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true,
                    modelRecordMapExpectedResult.get(modelGuid));
        }
    }

    private Map<String, List<GenericRecord>> calculateModelRecordMap(List<GenericRecord> outputRecords,
            String[] modelGuids) {
        Map<String, List<GenericRecord>> modelRecordMap = new HashMap<>();
        Stream.of(modelGuids).forEach((guid) -> modelRecordMap.put(guid, new ArrayList<>()));

        for (GenericRecord record : outputRecords) {
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            List<GenericRecord> perModelRecords = modelRecordMap.get(modelGuid);
            if (perModelRecords != null) {
                perModelRecords.add(record);
            }
        }
        return modelRecordMap;
    }

    private void verifyPerModelOutput(String modelGuid, List<GenericRecord> outputRecords, boolean expectedValue,
            List<GenericRecord> expectedResultsRecords) {
        Double prevRawScore = (expectedValue) ? Double.MAX_VALUE : 1.0;
        String scoreFieldName = (expectedValue) ? ScoreResultField.ExpectedRevenue.displayName
                : ScoreResultField.RawScore.displayName;
        Integer prevPct = 99;

        String keyColumn = "__Composite_Key__";
        Map<String, GenericRecord> outputRecordsMap = new HashMap<>();
        outputRecords.stream().forEach(r -> outputRecordsMap.put(r.get(keyColumn).toString(), r));
        Map<String, GenericRecord> expectedResultsRecordsMap = new HashMap<>();
        expectedResultsRecords.stream().forEach(r -> expectedResultsRecordsMap.put(r.get(keyColumn).toString(), r));

        for (GenericRecord record : outputRecords) {
            String recordModelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            Double curRawScore = (Double) record.get(scoreFieldName);
            Integer curPct = (Integer) record.get(ScoreResultField.ExpectedRevenuePercentile.displayName);

            assertEquals(recordModelGuid, modelGuid);
            assertTrue(curPct <= prevPct);
            assertTrue(curRawScore <= prevRawScore);

            assertTrue(curPct <= 99 && curPct >= 5, "Percentile " + curPct + " is not in range of [5, 99]");
            prevPct = curPct;
            prevRawScore = curRawScore;
        }

        assertEquals(outputRecordsMap.size(), expectedResultsRecordsMap.size());
        expectedResultsRecordsMap.keySet().forEach(k -> {
            GenericRecord outputRecord = outputRecordsMap.get(k);
            GenericRecord expectedResultRecord = expectedResultsRecordsMap.get(k);
            Double probability = (Double) outputRecord.get(ScoreResultField.Probability.displayName);
            Double predictedRevenue = (Double) outputRecord.get(ScoreResultField.PredictedRevenue.displayName);
            Double expectedRevenue = (Double) outputRecord.get(ScoreResultField.ExpectedRevenue.displayName);
            /*
            if (expectedRevenue != null) {
                Assert.assertEquals(expectedRevenue,
                        BigDecimal.valueOf(probability * predictedRevenue)
                                .setScale( //
                                        CalculateFittedExpectedRevenueFunction //
                                                .EV_REVENUE_PRECISION,
                                        RoundingMode.HALF_UP)
                                .doubleValue());
            }
            */
            expectedResultRecord.getSchema().getFields().stream().forEach(f -> {
                if (f.name().equals(ScoreResultField.ExpectedRevenuePercentile.displayName)
                        || f.name().equals(ScoreResultField.Percentile.displayName)) {
                    assertTrue(
                            Math.abs(Integer.parseInt(outputRecord.get(f.name()).toString())
                                    - Integer.parseInt(expectedResultRecord.get(f.name()).toString())) == 0,
                            String.format("record = %s, f.name() = %s, val = %s, expected = %s",
                                    expectedResultRecord.get(keyColumn).toString(), f.name(),
                                    Integer.parseInt(outputRecord.get(f.name()).toString()),
                                    Integer.parseInt(expectedResultRecord.get(f.name()).toString())));
                } else {
                    assertEquals(outputRecord.get(f.name()), expectedResultRecord.get(f.name()), f.name());
                }
            });
        });
    }

    @Override
    protected void postProcessSourceTable(Table table) {
        super.postProcessSourceTable(table);
    }

    private CalculateExpectedRevenuePercentileParameters prepareInputWithExpectedRevenue() {
        CalculateExpectedRevenuePercentileParameters parameters = new CalculateExpectedRevenuePercentileParameters();
        String predictedRevenueField = ScoreResultField.ExpectedRevenue.displayName;

        String modelGuidField = ScoreResultField.ModelId.displayName;

        String scoreField = ScoreResultField.ExpectedRevenuePercentile.displayName;

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put(evModelGuid, predictedRevenueField);

        parameters.setInputTableName("detailed");
        parameters.setPercentileFieldName(scoreField);
        parameters.setOriginalScoreFieldMap(rawScoreFieldMap);
        parameters.setModelGuidField(modelGuidField);
        parameters.setPercentileLowerBound(5);
        parameters.setPercentileUpperBound(99);
        
        Map<String, Double> normalizationRatioMap = new HashMap<>();
        normalizationRatioMap.put(evModelGuid, 1.23456D);
        parameters.setNormalizationRatioMap(normalizationRatioMap );

        setDummyScoreDerivationMap(parameters, evModelGuid);

        setFitFunctionParametersMap(parameters);
        String evFitFunctionStr = "{" //
                + "    \"ev\": {" //
                + "        \"alpha\": -80.38284301837075, " //
                + "        \"beta\": 267.3216400859408, " //
                + "        \"gamma\": 25.0, " //
                + "        \"maxRate\": 757.2290310578741, " //
                + "        \"version\": \"v2\"" //
                + "    }, " //
                + "    \"revenue\": {" //
                + "        \"alpha\": -36.11200826967415, " //
                + "        \"beta\": 174.34074995895384, " //
                + "        \"gamma\": 100, " //
                + "        \"maxRate\": 2057.8749609810993, " //
                + "        \"version\": \"v2\"" //
                + "    }, " //
                + "    \"probability\": {" //
                + "        \"alpha\": -229.07151039431125, " //
                + "        \"beta\": 1056.1946928457717, " //
                + "        \"gamma\": 100, " //
                + "        \"maxRate\": 0.38461538461538464, " //
                + "        \"version\": \"v2\"" //
                + "    }" //
                + "}";
        parameters.getFitFunctionParametersMap().put(evModelGuid, evFitFunctionStr);

        return parameters;
    }

    private void setDummyScoreDerivationMap(CalculateExpectedRevenuePercentileParameters parameters, String modelGuid) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("calculateExpectedRevenuePercentile/detailed/params.json");
        CalculateExpectedRevenuePercentileParameters tempParameters = JsonUtils.deserialize(inputStream,
                CalculateExpectedRevenuePercentileParameters.class);
        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = new HashMap<>();
        for (Map<ScoreDerivationType, ScoreDerivation> scoreDerivationMap : tempParameters.getScoreDerivationMaps()
                .values()) {
            scoreDerivationMaps.put(modelGuid, scoreDerivationMap);
            break;
        }
        parameters.setScoreDerivationMaps(scoreDerivationMaps);
    }

    @Override
    protected String getFlowBeanName() {
        return "calculateExpectedRevenuePercentile";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }
}
