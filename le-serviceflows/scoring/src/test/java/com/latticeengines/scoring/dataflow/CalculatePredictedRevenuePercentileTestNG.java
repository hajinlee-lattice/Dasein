package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.CalculatePredictedRevenuePercentileParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = {"classpath:serviceflows-scoring-dataflow-context.xml"})
public class CalculatePredictedRevenuePercentileTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testCalculationPredictedRevenuePercentile() {
        CalculatePredictedRevenuePercentileParameters parameters = prepareInputWithPredictedRevenue();
        executeDataFlow(parameters);
        verifyResults();
    }

    private void verifyResults() {
        List<GenericRecord> inputRecords = readInput("InputTable");
        List<GenericRecord> outputRecords = readOutput();

        assertEquals(outputRecords.size(), inputRecords.size());

        String[] modelGuids = {
            "ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw",
            "ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv",
            "ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql",
            "ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm",
        };

        Map<String, List<GenericRecord>> modelRecordMap = new HashMap<>();
        Stream.of(modelGuids).forEach((guid) -> modelRecordMap.put(guid, new ArrayList<>()));

        for (GenericRecord record : outputRecords) {
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            List<GenericRecord> perModelRecords = modelRecordMap.get(modelGuid);
            if (perModelRecords != null) {
                perModelRecords.add(record);
            }
        }

        assertEquals(3210, modelRecordMap.get("ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm").size());

        String[] evModelGuids = {
            "ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm",
        };

        for (String modelGuid : evModelGuids) {
            verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true);
        }

    }

    private void verifyPerModelOutput(String modelGuid, List<GenericRecord> outputRecords, boolean expectedValue) {
        Double prevRawScore = (expectedValue) ? Double.MAX_VALUE : 1.0;
        String scoreFieldName = (expectedValue) ? ScoreResultField.PredictedRevenue.displayName :
            ScoreResultField.RawScore.displayName;
        Integer prevPct = 99;

        for (GenericRecord record : outputRecords) {
            String recordModelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            Double curRawScore = (Double) record.get(scoreFieldName);
            Integer curPct = (Integer) record.get(ScoreResultField.PredictedRevenuePercentile.displayName);

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

    private CalculatePredictedRevenuePercentileParameters prepareInputWithPredictedRevenue() {
        CalculatePredictedRevenuePercentileParameters parameters = new CalculatePredictedRevenuePercentileParameters();
        String rawScoreField = ScoreResultField.RawScore.displayName;
        String predictedRevenueField = ScoreResultField.PredictedRevenue.displayName;

        String modelGuidField = ScoreResultField.ModelId.displayName;

        String scoreField = ScoreResultField.PredictedRevenuePercentile.displayName;

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put("ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", rawScoreField);
        rawScoreFieldMap.put("ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", rawScoreField);
        rawScoreFieldMap.put("ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql", rawScoreField);
        rawScoreFieldMap.put("ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm", predictedRevenueField);

        parameters.setInputTableName("InputTable");
        parameters.setPercentileFieldName(scoreField);
        parameters.setOriginalScoreFieldMap(rawScoreFieldMap);
        parameters.setModelGuidField(modelGuidField);
        parameters.setPercentileLowerBound(5);
        parameters.setPercentileUpperBound(99);

        return parameters;
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