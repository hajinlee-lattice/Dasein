package com.latticeengines.spark.exposed.job.score;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.avro.generic.GenericRecord;
import org.testng.annotations.Test;

import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.metadata.datastore.HdfsDataUnit;
import com.latticeengines.domain.exposed.pls.ModelType;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.serviceflows.scoring.spark.CombineInputTableWithScoreJobConfig;
import com.latticeengines.domain.exposed.spark.SparkJobResult;
import com.latticeengines.spark.testframework.SparkJobFunctionalTestNGBase;

public class CombineInputTableWithScoreInternalIdTestNG extends SparkJobFunctionalTestNGBase {

    private CombineInputTableWithScoreJobConfig getJobConfiguration() {
        CombineInputTableWithScoreJobConfig config = new CombineInputTableWithScoreJobConfig();
        config.idColumn = InterfaceName.InternalId.name();
        config.modelType = ModelType.PMML.name();
        return config;
    }

    @Override
    protected String getJobName() {
        return "combineInputTableWithScore";
    }

    @Override
    protected String getScenarioName() {
        return "internalIdBased";
    }

    @Override
    protected List<String> getInputOrder() {
        return Arrays.asList("InputTable", "ScoreResult");
    }

    @Test(groups = "functional")
    public void execute() throws IOException {
        SparkJobResult result = runSparkJob(CombineInputTableWithScoreJob.class, getJobConfiguration());
        verify(result, Collections.singletonList(this::verifyScoresAreCombined));
    }

    private Boolean verifyScoresAreCombined(HdfsDataUnit tgt) {
        List<GenericRecord> outputRecords = new ArrayList<>();
        verifyAndReadTarget(tgt).forEachRemaining(record -> {
            outputRecords.add(record);
        });
        assertEquals(outputRecords.size(), 2);
        for (GenericRecord record : outputRecords) {
            assertNotNull(record.get(InterfaceName.InternalId.name()));
            assertNotNull(record.get(ScoreResultField.Percentile.displayName));
        }
        return true;
    }

}
