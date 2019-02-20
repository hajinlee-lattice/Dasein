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
        List<GenericRecord> outputRecords = readOutput();

        assertEquals(outputRecords.size(), inputRecords.size());
        String[] modelGuids = { evModelGuid, };

        Map<String, List<GenericRecord>> modelRecordMap = new HashMap<>();
        Stream.of(modelGuids).forEach((guid) -> modelRecordMap.put(guid, new ArrayList<>()));

        for (GenericRecord record : outputRecords) {
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            List<GenericRecord> perModelRecords = modelRecordMap.get(modelGuid);
            if (perModelRecords != null) {
                perModelRecords.add(record);
            }
        }

        assertEquals(9465, modelRecordMap.get(evModelGuid).size());

        String[] evModelGuids = { evModelGuid, };

        for (String modelGuid : evModelGuids) {
            verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true);
        }
    }

    private void verifyPerModelOutput(String modelGuid, List<GenericRecord> outputRecords, boolean expectedValue) {
        Double prevRawScore = (expectedValue) ? Double.MAX_VALUE : 1.0;
        String scoreFieldName = (expectedValue) ? ScoreResultField.ExpectedRevenue.displayName
                : ScoreResultField.RawScore.displayName;
        Integer prevPct = 99;

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
    }

    @Override
    protected void postProcessSourceTable(Table table) {
        super.postProcessSourceTable(table);
    }

    private CalculateExpectedRevenuePercentileParameters prepareInputWithExpectedRevenue() {
        CalculateExpectedRevenuePercentileParameters parameters = new CalculateExpectedRevenuePercentileParameters();
        String rawScoreField = ScoreResultField.RawScore.displayName;
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
