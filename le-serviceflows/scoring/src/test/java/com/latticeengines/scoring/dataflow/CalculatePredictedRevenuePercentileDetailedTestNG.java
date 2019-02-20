package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculateExpectedRevenuePercentileParameters.ScoreDerivationType;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class CalculatePredictedRevenuePercentileDetailedTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testCalculationPredictedRevenuePercentile() {
        CalculatePredictedRevenuePercentileParameters parameters = prepareInputWithPredictedRevenue();
        executeDataFlow(parameters);
        verifyResults();
    }

    private void verifyResults() {
        List<GenericRecord> inputRecords = readInput("detailed");
        List<GenericRecord> outputRecords = readOutput();

        assertEquals(outputRecords.size(), inputRecords.size());

        String[] modelGuids = { "ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz" };

        Map<String, List<GenericRecord>> modelRecordMap = new HashMap<>();
        Stream.of(modelGuids).forEach((guid) -> modelRecordMap.put(guid, new ArrayList<>()));

        for (GenericRecord record : outputRecords) {
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            List<GenericRecord> perModelRecords = modelRecordMap.get(modelGuid);
            if (perModelRecords != null) {
                perModelRecords.add(record);
            }
        }

        assertEquals(9465, modelRecordMap.get("ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz").size());

        String[] evModelGuids = { "ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz", };

        for (String modelGuid : evModelGuids) {
            verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true);
        }

    }

    private void verifyPerModelOutput(String modelGuid, List<GenericRecord> outputRecords, boolean expectedValue) {
        Double prevRawScore = (expectedValue) ? Double.MAX_VALUE : 1.0;
        String scoreFieldName = (expectedValue) ? ScoreResultField.PredictedRevenue.displayName
                : ScoreResultField.RawScore.displayName;
        Integer prevPct = 99;

        for (GenericRecord record : outputRecords) {
            String recordModelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            Double curRawScore = (Double) record.get(scoreFieldName);
            Integer curPct = (Integer) record.get(ScoreResultField.PredictedRevenuePercentile.displayName);

            assertEquals(recordModelGuid, modelGuid);
            assertTrue(curPct <= prevPct);
            assertTrue(curRawScore <= prevRawScore,
                    String.format("modelGuid = %s, curPct = %s, prevPct = %s, curRawScore = %s, prevRawScore = %s",
                            modelGuid, curPct, prevPct, curRawScore, prevRawScore));

            assertTrue(curPct <= 99 && curPct >= 5, "Percentile " + curPct + " is not in range of [5, 99]");
            prevPct = curPct;
            prevRawScore = curRawScore;
        }
    }

    @Override
    protected void postProcessSourceTable(Table table) {
        super.postProcessSourceTable(table);
    }

    private CalculatePredictedRevenuePercentileParameters prepareInputWithPredictedRevenue() {
        CalculatePredictedRevenuePercentileParameters parameters = new CalculatePredictedRevenuePercentileParameters();
        String rawScoreField = ScoreResultField.RawScore.displayName;
        String predictedRevenueField = ScoreResultField.PredictedRevenue.displayName;

        String modelGuidField = ScoreResultField.ModelId.displayName;

        String scoreField = ScoreResultField.PredictedRevenuePercentile.displayName;
        String evModelGuid = "ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz";

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put(evModelGuid, predictedRevenueField);

        parameters.setInputTableName("detailed");
        parameters.setPercentileFieldName(scoreField);
        parameters.setOriginalScoreFieldMap(rawScoreFieldMap);
        parameters.setModelGuidField(modelGuidField);
        parameters.setPercentileLowerBound(5);
        parameters.setPercentileUpperBound(99);

        setDummyScoreDerivationMap(parameters, evModelGuid);

        return parameters;
    }

    private void setDummyScoreDerivationMap(CalculatePredictedRevenuePercentileParameters parameters,
            String modelGuid) {
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
        return "calculatePredictedRevenuePercentile";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }
}