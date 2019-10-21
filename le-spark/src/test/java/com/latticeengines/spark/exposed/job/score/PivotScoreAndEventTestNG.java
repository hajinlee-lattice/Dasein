package com.latticeengines.spark.exposed.job.score;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.csv.LECSVFormat;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class PivotScoreAndEventTestNG extends SparkJobFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(PivotScoreAndEventTestNG.class);

    private PivotScoreAndEventJobConfig getJobConfiguration() {
        PivotScoreAndEventJobConfig config = new PivotScoreAndEventJobConfig();
        String modelguid = "ms__f7f1eb16-0d26-4aa1-8c4a-3ac696e13d06-PLS_model";
        config.avgScores = ImmutableMap.of(modelguid, 0.05);
        config.scoreFieldMap = ImmutableMap.of(modelguid, InterfaceName.Event.name());
        return config;
    }

    @Override
    protected String getScenarioName() {
        return "ScoreOutput";
    }

    @Override
    protected String getJobName() {
        return "pivotScoreAndEvent";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable");
    }

    @Test(groups = "functional")
    public void execute() throws IOException {
        SparkJobResult result = runSparkJob(PivotScoreAndEventJob.class, getJobConfiguration());
        verify(result, Collections.singletonList(this::verifyPivotedScore));
    }

    private Boolean verifyPivotedScore(HdfsDataUnit tgt) {

        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });

        Assert.assertEquals(outputRecords.size(), 17);

        CSVFormat format = LECSVFormat.format.withSkipHeaderRecord(true);
        try (CSVParser parser = new CSVParser(new InputStreamReader(
                ClassLoader.getSystemResourceAsStream(String.format("%s/%s/pivot_score_event_result.csv", //
                        getJobName(), getScenarioName()))),
                format)) {
            List<CSVRecord> csvRecords = parser.getRecords();

            for (int i = 0; i < outputRecords.size(); i++) {
                CSVRecord csvRecord = csvRecords.get(i);
                GenericRecord avroRecord = outputRecords.get(i);
                log.info("avro:" + avroRecord);
                log.info("csv:" + csvRecord);
                for (String field : Arrays.asList("Score", "TotalEvents", "TotalPositiveEvents", "Lift")) {
                    Assert.assertEquals(Double.valueOf(csvRecord.get(field)),
                            Double.valueOf(avroRecord.get(field).toString()));
                }
            }
            return true;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

}
