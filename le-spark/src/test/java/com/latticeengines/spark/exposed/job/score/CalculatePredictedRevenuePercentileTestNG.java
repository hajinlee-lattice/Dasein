package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

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
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.scoringapi.ScoreDerivation;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig.ScoreDerivationType;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculatePredictedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalculatePredictedRevenuePercentileTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testCalculationPredictedRevenuePercentile() {
        SparkJobResult result = runSparkJob(CalculatePredictedRevenuePercentileJob.class,
                prepareInputWithPredictedRevenue());
        verify(result, Collections.singletonList(this::verifyResults));
    }

    private Boolean verifyResults(HdfsDataUnit tgt) {
        try {
            List<GenericRecord> inputRecords = readInput("InputTable");
            List<GenericRecord> outputRecords = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                outputRecords.add(record);
            });

            assertEquals(outputRecords.size(), inputRecords.size());

            String[] modelGuids = { "ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw",
                    "ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv",
                    "ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql",
                    "ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm", };

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

            String[] evModelGuids = { "ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm", };

            for (String modelGuid : evModelGuids) {
                verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true);
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return true;

    }

    public List<GenericRecord> readInput(String tableName) throws IOException {
        List<GenericRecord> inputRecords = new ArrayList<>();
        List<String> fileNames = Arrays.asList("part-v006-o000-00000.avro", "part-v006-o000-00001.avro",
                "part-v006-o000-00002.avro", "part-v006-o000-00004.avro");
        for (String fileName : fileNames) {
            String formatPath = String.format("%s/%s/%s/" + fileName, //
                    getJobName(), getScenarioName(), tableName);
            inputRecords.addAll(AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(formatPath) //
                    .getPath()));
        }
        return inputRecords;
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

    private CalculatePredictedRevenuePercentileJobConfig prepareInputWithPredictedRevenue() {
        CalculatePredictedRevenuePercentileJobConfig config = new CalculatePredictedRevenuePercentileJobConfig();
        String rawScoreField = ScoreResultField.RawScore.displayName;
        String predictedRevenueField = ScoreResultField.PredictedRevenue.displayName;

        String modelGuidField = ScoreResultField.ModelId.displayName;

        String scoreField = ScoreResultField.PredictedRevenuePercentile.displayName;
        String evModelGuid = "ms__73d85df6-688e-4368-948b-65f3688cc7ea-ai_0tlcm";

        Map<String, String> rawScoreFieldMap = new HashMap<>();
        rawScoreFieldMap.put("ms__ed222df9-bd34-4449-b71d-563162464123-ai__ppqw", rawScoreField);
        rawScoreFieldMap.put("ms__92fc828f-11eb-4188-9da8-e6f2c9cc35c8-ai_ukuiv", rawScoreField);
        rawScoreFieldMap.put("ms__8769cf68-d174-4427-916d-1ef19db02f0a-ai_nabql", rawScoreField);
        rawScoreFieldMap.put(evModelGuid, predictedRevenueField);

        config.percentileFieldName = scoreField;
        config.originalScoreFieldMap = rawScoreFieldMap;
        config.modelGuidField = modelGuidField;
        config.percentileLowerBound = 5;
        config.percentileUpperBound = 99;

        setDummyScoreDerivationMap(config, evModelGuid);

        return config;
    }

    private void setDummyScoreDerivationMap(CalculatePredictedRevenuePercentileJobConfig config, String modelGuid) {
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("calculateExpectedRevenuePercentileJob/PLS-11356/params.json");
        CalculateExpectedRevenuePercentileJobConfig tempParameters = JsonUtils.deserialize(inputStream,
                CalculateExpectedRevenuePercentileJobConfig.class);
        Map<String, Map<ScoreDerivationType, ScoreDerivation>> scoreDerivationMaps = new HashMap<>();
        for (Map<ScoreDerivationType, ScoreDerivation> scoreDerivationMap : tempParameters.scoreDerivationMaps
                .values()) {
            scoreDerivationMaps.put(modelGuid, scoreDerivationMap);
            break;
        }
        config.scoreDerivationMaps = scoreDerivationMaps;
    }

    @Override
    protected String getJobName() {
        return "calculatePredictedRevenuePercentile";
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
    protected void customConfig(Map<String, Object> conf, Map<String, String> sparkConf) {
        conf.put("driverMemory", "2g");
        sparkConf.put("spark.default.parallelism", String.valueOf(5));
        sparkConf.put("spark.sql.shuffle.partitions", String.valueOf(5));
        sparkConf.put("spark.driver.maxResultSize", "1g");
    }

}
