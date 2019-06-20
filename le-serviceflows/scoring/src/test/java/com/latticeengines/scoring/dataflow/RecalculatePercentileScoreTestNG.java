package com.latticeengines.scoring.dataflow;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.springframework.test.context.ContextConfiguration;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.Table;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.dataflow.RecalculatePercentileScoreParameters;
import com.latticeengines.serviceflows.functionalframework.ServiceFlowsDataFlowFunctionalTestNGBase;

@ContextConfiguration(locations = { "classpath:serviceflows-scoring-dataflow-context.xml" })
public class RecalculatePercentileScoreTestNG extends ServiceFlowsDataFlowFunctionalTestNGBase {

    @Test(groups = "functional")
    public void test() {
        FileUtils.deleteQuietly(new File("/tmp/ppqw.json"));
        FileUtils.deleteQuietly(new File("/tmp/ukuiv.json"));
        FileUtils.deleteQuietly(new File("/tmp/nabql.json"));
        RecalculatePercentileScoreParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
        assertTrue(new File("/tmp/ppqw.json").exists());
        assertTrue(new File("/tmp/ukuiv.json").exists());
        assertTrue(!new File("/tmp/nabql.json").exists());
    }

    @Test(groups = "functional", dependsOnMethods = "test")
    public void testWithTargetScoreDerivation() {
        RecalculatePercentileScoreParameters parameters = prepareInput();
        executeDataFlow(parameters);
        verifyResult();
    }

    private void verifyResult() {
        List<GenericRecord> inputRecords = readInput("InputTable");
        List<GenericRecord> outputRecords = readOutput();

        assertEquals(outputRecords.size(), inputRecords.size());

        String[] modelGuids = { "ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", // 5324
                "ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", // 3012
                "ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql" }; // 7733

        Map<String, List<GenericRecord>> modelRecordMap = new HashMap<>();
        Stream.of(modelGuids).forEach((guid) -> modelRecordMap.put(guid, new ArrayList<>()));

        for (GenericRecord record : outputRecords) {
            String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            List<GenericRecord> perModelRecords = modelRecordMap.get(modelGuid);
            assertNotNull(perModelRecords);
            perModelRecords.add(record);
        }

        for (String modelGuid : modelGuids) {
            verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid));
        }

    }

    private void verifyPerModelOutput(String modelGuid, List<GenericRecord> outputRecords) {
        Double prevRawScore = 1.0;
        Integer prevPct = 99;

        for (GenericRecord record : outputRecords) {
            String recordModelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            Double curRawScore = (Double) record.get(ScoreResultField.RawScore.displayName);
            Integer curPct = (Integer) record.get(ScoreResultField.Percentile.displayName);

            assertEquals(recordModelGuid, modelGuid);
            assertTrue(curPct <= prevPct, modelGuid);
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

    private RecalculatePercentileScoreParameters prepareInput() {
        RecalculatePercentileScoreParameters parameters = new RecalculatePercentileScoreParameters();
        String rawScoreField = ScoreResultField.RawScore.displayName;

        String modelGuidField = ScoreResultField.ModelId.displayName;

        String scoreField = ScoreResultField.Percentile.displayName;

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put("ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", rawScoreField);
        rawScoreFieldMap.put("ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", rawScoreField);
        rawScoreFieldMap.put("ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql",
                ScoreResultField.ExpectedRevenue.displayName);

        parameters.setInputTableName("InputTable");
        parameters.setRawScoreFieldName(rawScoreField);
        parameters.setScoreFieldName(scoreField);
        parameters.setModelGuidField(modelGuidField);
        parameters.setPercentileLowerBound(5);
        parameters.setPercentileUpperBound(99);
        parameters.setOriginalScoreFieldMap(rawScoreFieldMap);
        parameters.setTargetScoreDerivation(true);
        Map<String, String> targetScoreDerivationPaths = new HashMap<>();
        targetScoreDerivationPaths.put("ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", "file:///tmp/ppqw.json");
        targetScoreDerivationPaths.put("ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", "file:///tmp/ukuiv.json");
        targetScoreDerivationPaths.put("ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql", "file:///tmp/nabql.json");
        parameters.setTargetScoreDerivationPaths(targetScoreDerivationPaths);

        return parameters;
    }

    @Override
    protected String getFlowBeanName() {
        return "recalculatePercentileScore";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }
}
