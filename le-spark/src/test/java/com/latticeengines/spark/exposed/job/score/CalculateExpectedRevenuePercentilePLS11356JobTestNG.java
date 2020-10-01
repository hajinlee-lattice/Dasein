package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.avro.generic.GenericRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CalculateExpectedRevenuePercentileJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CalculateExpectedRevenuePercentilePLS11356JobTestNG extends SparkJobFunctionalTestNGBase {

    @Override
    protected String getJobName() {
        return "calculateExpectedRevenuePercentileJob";
    }

    @Override
    protected String getScenarioName() {
        return "PLS-11356";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable");
    }

    @Test(groups = "functional")
    public void testCalculationExpectedRevenuePercentile() {
        SparkJobResult result = runSparkJob(CalculateExpectedRevenuePercentileJob.class,
                prepareInputWithExpectedRevenue());
        verify(result, Collections.singletonList(this::verifyResults));
    }

    private Boolean verifyResults(HdfsDataUnit tgt) {
        List<GenericRecord> inputRecords = readInput();
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });

        assertEquals(outputRecords.size(), inputRecords.size());
        AtomicInteger evRecordCount = new AtomicInteger(0);
        AtomicInteger nonEvRecordCount = new AtomicInteger(0);
        outputRecords.forEach(record -> {
            Assert.assertNotNull(record.get(ScoreResultField.Percentile.displayName));
            if (record.get(ScoreResultField.ExpectedRevenuePercentile.displayName) != null) {
                Assert.assertEquals(record.get(ScoreResultField.Percentile.displayName),
                        record.get(ScoreResultField.ExpectedRevenuePercentile.displayName));
                Assert.assertNotNull(record.get(ScoreResultField.ExpectedRevenue.displayName));
                evRecordCount.getAndIncrement();
            } else {
                Assert.assertNull(record.get(ScoreResultField.ExpectedRevenue.displayName));
                nonEvRecordCount.getAndIncrement();
            }
        });
        Assert.assertEquals(evRecordCount.get(), 19179);
        Assert.assertEquals(nonEvRecordCount.get(), 103457);
        return true;
    }

    public List<GenericRecord> readInput() {
        try {
            List<GenericRecord> inputRecords = new ArrayList<>();
            List<String> fileNames = Arrays.asList("part-v025-o000-00000.avro");
            for (String fileName : fileNames) {
                String formatPath = String.format("%s/%s/%s/" + fileName, //
                        getJobName(), getScenarioName(), "InputTable");
                inputRecords.addAll(AvroUtils.readFromLocalFile(ClassLoader.getSystemResource(formatPath) //
                        .getPath()));
            }
            return inputRecords;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private CalculateExpectedRevenuePercentileJobConfig prepareInputWithExpectedRevenue() {
        InputStream inputStream = Thread.currentThread().getContextClassLoader() //
                .getResourceAsStream("calculateExpectedRevenuePercentileJob/PLS-11356/params.json");
        CalculateExpectedRevenuePercentileJobConfig config = JsonUtils.deserialize(inputStream,
                CalculateExpectedRevenuePercentileJobConfig.class);

        CalculateScoreTestUtils.setFitFunctionParametersMap(config);
        return config;
    }

}
