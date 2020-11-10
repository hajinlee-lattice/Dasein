package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculatePercentileScoreJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class RecalculatePercentileScoreJobTestNG extends SparkJobFunctionalTestNGBase {

    String derivationStr = null;
    String tempFile = "/tmp/targetScoreDerivation.json";

    @Test(groups = "functional")
    public void test() throws Exception {
        FileUtils.deleteQuietly(new File(tempFile));
        SparkJobResult result = runSparkJob(RecalculatePercentileScoreJob.class, prepareInput());
        verify(result, Collections.singletonList(this::verifyResult));
        FileUtils.writeStringToFile(new File(tempFile), derivationStr, "UTF-8");
        System.out.println("Finished verify1.");
        assertTrue(new File(tempFile).exists());

    }

    @Test(groups = "functional", dependsOnMethods = "test")
    public void testWithTargetScoreDerivation() throws Exception {
        File file = new File(tempFile);
        if (file.exists()) {
            derivationStr = FileUtils.readFileToString(file, "UTF-8");
        }
        SparkJobResult result = runSparkJob(RecalculatePercentileScoreJob.class, prepareInput());
        verify(result, Collections.singletonList(this::verifyResult));
        FileUtils.deleteQuietly(new File(tempFile));
        System.out.println("Finished verify2.");
    }

    private Boolean verifyResult(HdfsDataUnit tgt) {
        try {
            List<GenericRecord> inputRecords = readInput();
            List<GenericRecord> outputRecords = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                outputRecords.add(record);
            });

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
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        return true;

    }

    public List<GenericRecord> readInput() throws IOException {
        List<GenericRecord> inputRecords = new ArrayList<>();
        List<String> fileNames = Arrays.asList("part-v006-o000-00000.avro", "part-v006-o000-00001.avro",
                "part-v006-o000-00002.avro");
        for (String fileName : fileNames) {
            String formatPath = String.format("%s/%s/%s/" + fileName, //
                    getJobName(), getScenarioName(), "InputTable");
            inputRecords.addAll(AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(formatPath) //
                    .getPath()));
        }
        return inputRecords;
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

    private RecalculatePercentileScoreJobConfig prepareInput() {
        RecalculatePercentileScoreJobConfig config = new RecalculatePercentileScoreJobConfig();
        String rawScoreField = ScoreResultField.RawScore.displayName;
        String modelGuidField = ScoreResultField.ModelId.displayName;
        String scoreField = ScoreResultField.Percentile.displayName;

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put("ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", rawScoreField);
        rawScoreFieldMap.put("ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", rawScoreField);
        rawScoreFieldMap.put("ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql",
                ScoreResultField.ExpectedRevenue.displayName);

        config.inputTableName = "InputTable";
        config.rawScoreFieldName = rawScoreField;
        config.scoreFieldName = scoreField;
        config.modelGuidField = modelGuidField;
        config.percentileLowerBound = 5;
        config.percentileUpperBound = 99;
        config.originalScoreFieldMap = rawScoreFieldMap;
        config.targetScoreDerivation = true;

        Map<String, String> targetScoreDerivationInputs = new HashMap<>();
        if (derivationStr != null) {
            Map<String, String> derivationMap = JsonUtils.deserialize(derivationStr,
                    new TypeReference<Map<String, String>>() {
                    });
            for (String key : derivationMap.keySet()) {
                ScoreDerivation der = JsonUtils.deserialize(derivationMap.get(key), ScoreDerivation.class);
                targetScoreDerivationInputs.put(key, JsonUtils.serialize(der));
            }
        }
        config.targetScoreDerivationInputs = targetScoreDerivationInputs;
        return config;
    }

    @Override
    protected String getJobName() {
        return "recalculatePercentileScoreJob";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable");
    }

    @Override
    protected void verifyOutput(String output) {
        derivationStr = output;
        System.out.println("derivationStr=" + derivationStr);
    }

}
