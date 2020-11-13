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
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.RecalculateExpectedRevenueJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class RecalculateExpectedRevenueDetailedJobTestNG extends SparkJobFunctionalTestNGBase {

    @Test(groups = "functional")
    public void testCalculationPredictedRevenue() {
        SparkJobResult result = runSparkJob(RecalculateExpectedRevenueJob.class, prepareInputWithPredictedRevenue());
        verify(result, Collections.singletonList(this::verifyResult));
    }

    private Boolean verifyResult(HdfsDataUnit tgt) {
        try {
            List<GenericRecord> inputRecords = readInput("detailed");
            List<GenericRecord> expectedResultsRecords = readInput("expectedResult");
            List<GenericRecord> outputRecords = new ArrayList<>();
            verifyAndReadTarget(tgt).forEachRemaining(record -> {
                outputRecords.add(record);
            });

            assertEquals(outputRecords.size(), inputRecords.size());
            assertEquals(outputRecords.size(), expectedResultsRecords.size());

            String[] modelGuids = { "ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz" };
            Map<String, List<GenericRecord>> modelRecordMap = calculateModelRecordMap(outputRecords, modelGuids);
            Map<String, List<GenericRecord>> modelRecordMapExpectedResult = calculateModelRecordMap(
                    expectedResultsRecords, modelGuids);

            assertEquals(9465, modelRecordMap.get("ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz").size());
            assertEquals(9465,
                    modelRecordMapExpectedResult.get("ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz").size());
            String[] evModelGuids = { "ms__cbfbabb0-743b-4b7a-bb01-d12b2d029532-ai_btnmz", };
            for (String modelGuid : evModelGuids) {
                verifyPerModelOutput(modelGuid, modelRecordMap.get(modelGuid), true,
                        modelRecordMapExpectedResult.get(modelGuid));
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return true;

    }

    public List<GenericRecord> readInput(String tableName) throws IOException {
        List<GenericRecord> inputRecords = new ArrayList<>();
        List<String> fileNames = Arrays.asList("part-00000.avro");
        for (String fileName : fileNames) {
            String formatPath = String.format("%s/%s/%s/" + fileName, //
                    getJobName(), getScenarioName(), tableName);
            inputRecords.addAll(AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(formatPath) //
                    .getPath()));
        }
        return inputRecords;
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
        String scoreFieldName = (expectedValue) ? ScoreResultField.PredictedRevenue.displayName
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

        assertEquals(outputRecordsMap.size(), expectedResultsRecordsMap.size());
        expectedResultsRecordsMap.keySet().forEach(k -> {
            GenericRecord outputRecord = outputRecordsMap.get(k);
            GenericRecord expectedResultRecord = expectedResultsRecordsMap.get(k);

            expectedResultRecord.getSchema().getFields().stream().forEach(f -> {
                if (f.name().equals(ScoreResultField.Probability.displayName)) {
                    Assert.assertTrue((Double) outputRecord.get(f.name()) < 1D);
                    Assert.assertTrue((Double) outputRecord.get(f.name()) > 0D);
                }
                assertEquals(outputRecord.get(f.name()), expectedResultRecord.get(f.name()), f.name());
            });
        });
    }

    private RecalculateExpectedRevenueJobConfig prepareInputWithPredictedRevenue() {

        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("recalculateExpectedRevenue/multiModel/params.json");
        RecalculateExpectedRevenueJobConfig config = JsonUtils.deserialize(inputStream,
                RecalculateExpectedRevenueJobConfig.class);
        config.inputTableName = "detailed";
        return config;
    }

    @Override
    protected String getJobName() {
        return "recalculateExpectedRevenue";
    }

    @Override
    protected String getScenarioName() {
        return "multiModel";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("detailed");
    }
}
