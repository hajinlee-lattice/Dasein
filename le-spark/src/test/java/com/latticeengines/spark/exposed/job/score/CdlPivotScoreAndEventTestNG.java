package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.PivotScoreAndEventJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CdlPivotScoreAndEventTestNG extends SparkJobFunctionalTestNGBase {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CdlPivotScoreAndEventTestNG.class);

    private PivotScoreAndEventJobConfig getJobConfiguration() {
        PivotScoreAndEventJobConfig config = new PivotScoreAndEventJobConfig();
        String modelguid1 = "ms__71fa73d2-ce4b-483a-ab1a-02e4471cd0fc-RatingEn";
        String modelguid2 = "ms__af81bb1f-a71e-4b3a-89cd-3a9d0c02b0d1-CDLEnd2E";
        config.scoreFieldMap = ImmutableMap.of( //
                modelguid1, InterfaceName.ExpectedRevenue.name(), //
                modelguid2, InterfaceName.RawScore.name() //
        );
        return config;
    }

    @Override
    protected String getJobName() {
        return "pivotScoreAndEvent";
    }

    @Override
    protected String getScenarioName() {
        return "CDLScoreOutput";
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

        assertEquals(outputRecords.size(), 69);

        return true;
    }
}
