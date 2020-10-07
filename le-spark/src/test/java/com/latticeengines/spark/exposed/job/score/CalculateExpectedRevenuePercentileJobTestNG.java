package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
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
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalculateExpectedRevenuePercentileJobTestNG extends SparkJobFunctionalTestNGBase {

    String evModelGuid = "ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm";
    String derivationStr = null;
    String tempFile = "/tmp/0tlcm.json";

    @Test(groups = "functional")
    public void testCalculationExpectedRevenuePercentile() throws Exception {
        FileUtils.deleteQuietly(new File(tempFile));
        SparkJobResult result = runSparkJob(CalculateExpectedRevenuePercentileJob.class,
                prepareInputWithExpectedRevenue());
        verify(result, Collections.singletonList(this::verifyResults));
        FileUtils.writeStringToFile(new File(tempFile), derivationStr, "UTF-8");
        System.out.println("Finished verify1.");
    }

    @Test(groups = "functional", dependsOnMethods = "testCalculationExpectedRevenuePercentile")
    public void testCalculationExpectedRevenuePercentileUsingTargetDerivation() throws Exception {

        File file = new File(tempFile);
        if (file.exists()) {
            derivationStr = FileUtils.readFileToString(file, "UTF-8");
        }
        SparkJobResult result = runSparkJob(CalculateExpectedRevenuePercentileJob.class,
                prepareInputWithExpectedRevenue());
        verify(result, Collections.singletonList(this::verifyResults));
        FileUtils.deleteQuietly(new File(tempFile));
        System.out.println("Finished verify2.");
    }

    private Boolean verifyResults(HdfsDataUnit tgt) {

        try {
            List<GenericRecord> inputRecords = readInput();

            List<GenericRecord> outputRecords = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                outputRecords.add(record);
            });

            assertEquals(outputRecords.size(), inputRecords.size());
            String[] modelGuids = { //
                    "ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", //
                    "ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", //
                    "ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql", //
                    evModelGuid, };

            Map<String, List<GenericRecord>> modelRecordMap = new HashMap<>();
            Stream.of(modelGuids).forEach((guid) -> modelRecordMap.put(guid, new ArrayList<>()));

            for (GenericRecord record : outputRecords) {
                String modelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
                List<GenericRecord> perModelRecords = modelRecordMap.get(modelGuid);
                if (perModelRecords != null) {
                    perModelRecords.add(record);
                }
            }
            assertEquals(modelRecordMap.get(evModelGuid).size(), 2399);
            String[] evModelGuids = { evModelGuid, };
            for (String modelGuid : evModelGuids) {
                verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true);
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
                "part-v006-o000-00002.avro", "part-v006-o000-00004.avro");
        for (String fileName : fileNames) {
            String formatPath = String.format("%s/%s/%s/" + fileName, //
                    getJobName(), getScenarioName(), "InputTable");
            inputRecords.addAll(AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(formatPath) //
                    .getPath()));
        }
        return inputRecords;
    }

    private void verifyPerModelOutput(String modelGuid, List<GenericRecord> outputRecords, boolean expectedValue) {
        Double prevRawScore = (expectedValue) ? Double.MAX_VALUE : 1.0;
        String scoreFieldName = (expectedValue) ? ScoreResultField.ExpectedRevenue.displayName
                : ScoreResultField.RawScore.displayName;
        Collections.sort(outputRecords, (a, b) -> {
            Double score1 = (Double) a.get(scoreFieldName);
            Double score2 = (Double) b.get(scoreFieldName);
            return Double.compare(score2, score1);
        });
        Integer prevPct = 99;
        for (GenericRecord record : outputRecords) {
            String recordModelGuid = record.get(ScoreResultField.ModelId.displayName).toString();
            Double curRawScore = (Double) record.get(scoreFieldName);
            Integer curPct = (Integer) record.get(ScoreResultField.ExpectedRevenuePercentile.displayName);
            assertEquals(recordModelGuid, modelGuid);
            assertTrue(curRawScore <= prevRawScore);
            assertTrue(curPct <= prevPct);
            assertTrue(curPct <= 99 && curPct >= 5, "Percentile " + curPct + " is not in range of [5, 99]");
            prevPct = curPct;
            prevRawScore = curRawScore;
        }
    }

    private CalculateExpectedRevenuePercentileJobConfig prepareInputWithExpectedRevenue() {
        CalculateExpectedRevenuePercentileJobConfig config = new CalculateExpectedRevenuePercentileJobConfig();
        String rawScoreField = ScoreResultField.RawScore.displayName;
        String predictedRevenueField = ScoreResultField.ExpectedRevenue.displayName;

        String modelGuidField = ScoreResultField.ModelId.displayName;

        String scoreField = ScoreResultField.ExpectedRevenuePercentile.displayName;

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put("ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", rawScoreField);
        rawScoreFieldMap.put("ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", rawScoreField);
        rawScoreFieldMap.put("ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql", rawScoreField);
        rawScoreFieldMap.put(evModelGuid, predictedRevenueField);

        config.inputTableName = "InputTable";
        config.percentileFieldName = scoreField;
        config.originalScoreFieldMap = rawScoreFieldMap;
        config.modelGuidField = modelGuidField;
        config.percentileLowerBound = 5;
        config.percentileUpperBound = 99;

        Map<String, Double> normalizationRatioMap = new HashMap<>();
        normalizationRatioMap.put(evModelGuid, 1.23456D);
        config.normalizationRatioMap = normalizationRatioMap;

        setDummyScoreDerivationMap(config, evModelGuid);
        CalculateScoreTestUtils.setFitFunctionParametersMap(config);

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

    private void setDummyScoreDerivationMap(CalculateExpectedRevenuePercentileJobConfig config, String modelGuid) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("calculateExpectedRevenuePercentileJob/PLS-11356/params.json");
        CalculateExpectedRevenuePercentileJobConfig tempConfig = JsonUtils.deserialize(inputStream,
                CalculateExpectedRevenuePercentileJobConfig.class);
        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = new HashMap<>();
        for (Map<ScoreDerivationType, ScoreDerivation> scoreDerivationMap : tempConfig.scoreDerivationMaps.values()) {
            scoreDerivationMaps.put(modelGuid, scoreDerivationMap);
            break;
        }
        config.scoreDerivationMaps = scoreDerivationMaps;
    }

    @Override
    protected String getJobName() {
        return "calculateExpectedRevenuePercentileJob";
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

    @Override
    protected void customConfig(Map<String, Object> conf, Map<String, String> sparkConf) {
        sparkConf.put("spark.default.parallelism", String.valueOf(5));
        sparkConf.put("spark.sql.shuffle.partitions", String.valueOf(5));
    }
}
